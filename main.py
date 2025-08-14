import time
import os
from multiprocessing import Process
from typing import Any, Dict, List
import logging

from api import app
from config import load_config, HOST_ADDRESS, HOST_PORT, DLT_BASE_URL
from utils.node_monitor.health_checker import NodeHealthChecker
from utils.workers.worker_pool import WorkerPool

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s in %(module)s: %(message)s')
logger = logging.getLogger(__name__)

def node_list_setup():
    """
    Set up Redis connection variables for node list storage.
    """
    redis_config = {
        'host': os.getenv('REDIS_HOST', 'catalogue-coordinator-redis'),
        'port': int(os.getenv('REDIS_PORT', 6379)),
        'db': int(os.getenv('REDIS_DB', 0)),
        'key': 'nodes',
    }
    return redis_config

def setup_server():
    logger.info(f"Starting Flask API server at {HOST_ADDRESS}:{HOST_PORT}")
    app.run(host=HOST_ADDRESS, port=HOST_PORT)

def setup_node_monitoring(redis_config: Dict[str, Any]):
    """
    Set up and start node health monitoring.
    """
    logger.info("Setting up node health monitoring")
    health_checker = NodeHealthChecker(redis_config, grace_period=60)
    health_checker.start_monitoring()

def setup_worker_pool(redis_config: Dict[str, Any]):
    """
    Set up and start the worker pool.
    """
    logger.info("Setting up worker pool")
    worker_pool = WorkerPool()
    worker_pool.start()
    
    # Cache to track processed offerings and prevent reprocessing
    processed_offerings_cache = set()
    
    # Submit initial offering processing task
    try:
        from utils import get_offerings_for_processing
        offerings = get_offerings_for_processing(redis_config)
        
        if offerings:
            logger.info(f"Found {len(offerings)} offerings to process")
            # Filter out already processed offerings
            new_offerings = []
            for offering in offerings:
                offering_id = offering.get('name')
                if offering_id and offering_id not in processed_offerings_cache:
                    new_offerings.append(offering)
                    processed_offerings_cache.add(offering_id)
            
            if new_offerings:
                task_ids = worker_pool.submit_bulk_offering_processing(new_offerings, redis_config)
                logger.info(f"Submitted {len(task_ids)} new offering processing tasks")
            else:
                logger.info("All offerings already processed")
        else:
            logger.info("No offerings found to process initially")
            
    except Exception as e:
        logger.error(f"Error setting up initial offering processing: {e}")
    
    # Keep the worker pool running and periodically check for new offerings
    try:
        while True:
            time.sleep(60)  # Check every minute for new offerings
            
            try:
                # Check for new offerings and submit processing tasks
                offerings = get_offerings_for_processing(redis_config)
                if offerings:
                    # Filter out already processed offerings
                    new_offerings = []
                    for offering in offerings:
                        offering_id = offering.get('name')
                        if offering_id and offering_id not in processed_offerings_cache:
                            new_offerings.append(offering)
                            processed_offerings_cache.add(offering_id)
                    
                    if new_offerings:
                        task_ids = worker_pool.submit_bulk_offering_processing(new_offerings, redis_config)
                        logger.info(f"Submitted {len(task_ids)} new offering processing tasks")
                    else:
                        logger.debug("No new offerings to process")
                
                # Auto-cleanup completed tasks to prevent memory leaks
                worker_pool.auto_cleanup(max_completed_tasks=50)
                        
            except Exception as e:
                logger.error(f"Error checking for new offerings: {e}")
                
    except KeyboardInterrupt:
        logger.info("Stopping worker pool")
        worker_pool.stop()

def main():
    """
    Catalogue Co-ordinator entry point.
    """
    logger.info("Starting Catalogue Coordinator")
    
    load_config()
    
    # Set up Redis configuration
    redis_config = node_list_setup()
    
    # Start the API server in a separate process
    server_process = Process(target=setup_server, name="FlaskAPI")
    server_process.start()
    logger.info("API server process started")
    
    # Start node monitoring in a separate process
    monitor_process = Process(target=setup_node_monitoring, args=(redis_config,), name="NodeMonitor")
    monitor_process.start()
    logger.info("Node monitoring process started")
    
    # Start worker pool in a separate process
    worker_process = Process(target=setup_worker_pool, args=(redis_config,), name="WorkerPool")
    worker_process.start()
    logger.info("Worker pool process started")
    
    # Main process keeps all subprocesses alive
    try:
        while True:
            # Check if all processes are still alive
            if not server_process.is_alive():
                logger.error("API server process died, restarting...")
                server_process = Process(target=setup_server, name="FlaskAPI")
                server_process.start()
                logger.info("API server process restarted")
            
            if not monitor_process.is_alive():
                logger.error("Node monitoring process died, restarting...")
                monitor_process = Process(target=setup_node_monitoring, args=(redis_config,), name="NodeMonitor")
                monitor_process.start()
                logger.info("Node monitoring process restarted")
            
            if not worker_process.is_alive():
                logger.error("Worker pool process died, restarting...")
                worker_process = Process(target=setup_worker_pool, args=(redis_config,), name="WorkerPool")
                worker_process.start()
                logger.info("Worker pool process restarted")
            
            time.sleep(5)  # Check every 5 seconds
            
    except KeyboardInterrupt:
        logger.info("Shutting down Catalogue Coordinator...")
        
        # Terminate all processes gracefully
        for process in [server_process, monitor_process, worker_process]:
            if process.is_alive():
                process.terminate()
                process.join(timeout=5)
                if process.is_alive():
                    logger.warning(f"Force killing {process.name}")
                    process.kill()
                    process.join()
        
        logger.info("All processes terminated")

if __name__ == "__main__":
    main()