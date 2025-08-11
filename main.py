import time
from multiprocessing import Process
from typing import Any, Dict
import logging
import os

from cachetools import TTLCache

from api import app
from config import (
    HOST_ADDRESS,
    HOST_PORT,
    FETCH_INTERVAL_SECONDS,
    NODE_CHECK_INTERVAL_SECONDS,
    REDIS_HOST,
    REDIS_PORT,
    REDIS_DB,
    REDIS_NODES_KEY,
)
from utils.dlt_comm.get_nodes import get_node_list, fetch_and_store_nodes
from utils.monitoring import NodeMonitor
from utils.workers import OfferingWorker

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s in %(module)s: %(message)s')
logger = logging.getLogger(__name__)


def node_list_setup() -> Dict[str, Any]:
    """Set up Redis connection configuration."""
    return {
        'host': REDIS_HOST,
        'port': REDIS_PORT,
        'db': REDIS_DB,
        'key': REDIS_NODES_KEY,
    }


def setup_server():
    """Start the Flask API server."""
    logger.info(f"Starting Flask API server at {HOST_ADDRESS}:{HOST_PORT}")
    app.run(host=HOST_ADDRESS, port=HOST_PORT)


def main():
    """
    Catalogue Co-ordinator entry point.
    """
    redis_config = node_list_setup()
    
    # Prime placeholder nodes
    fetch_and_store_nodes(redis_config)

    # Start node monitor
    monitor = NodeMonitor(
        get_node_list, 
        redis_config, 
        check_interval=NODE_CHECK_INTERVAL_SECONDS
    )
    monitor.start()

    # Start offerings worker
    worker = OfferingWorker(interval_seconds=FETCH_INTERVAL_SECONDS)
    worker.start()

    # Start API server in separate process
    server_process = Process(target=setup_server)
    server_process.start()
    logger.info("API server process started.")

    # Main process keeps monitoring and worker alive
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down Coordinator...")
        monitor.stop()
        worker.stop()
        server_process.terminate()
        server_process.join()


if __name__ == "__main__":
    main()