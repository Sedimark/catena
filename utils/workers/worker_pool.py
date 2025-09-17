import queue
import time
import logging
from typing import Dict, List, Any, Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
import os

logger = logging.getLogger(__name__)

class WorkerPool:
    """
    Thread pool for handling multithreaded operations with DLT Booth and Catalogues.
    """
    
    def __init__(self, max_workers: int = None):
        self.max_workers = max_workers or int(os.getenv('WORKER_POOL_SIZE', 10))
        self.executor = ThreadPoolExecutor(max_workers=self.max_workers)
        self.task_queue = queue.Queue()
        self.results = {}
        self.is_running = False
        self.worker_threads = []
        
        logger.info(f"Worker pool initialized with {self.max_workers} workers")
    
    def submit_task(self, task_func: Callable, *args, **kwargs) -> str:
        """
        Submit a task to the worker pool.
        Returns a task ID for tracking.
        """
        if not self.is_running:
            self.start()
        
        task_id = f"task_{int(time.time() * 1000)}"
        future = self.executor.submit(task_func, *args, **kwargs)
        
        # Store future for result retrieval
        self.results[task_id] = {
            'future': future,
            'status': 'pending',
            'result': None,
            'error': None
        }
        
        logger.debug(f"Task {task_id} submitted to worker pool")
        return task_id
    
    def submit_batch(self, tasks: List[tuple]) -> List[str]:
        """
        Submit multiple tasks at once.
        Each task should be a tuple: (task_func, args, kwargs)
        """
        task_ids = []
        for task_func, args, kwargs in tasks:
            task_id = self.submit_task(task_func, *args, **kwargs)
            task_ids.append(task_id)
        
        logger.info(f"Submitted {len(tasks)} tasks to worker pool")
        return task_ids
    
    def get_task_result(self, task_id: str, timeout: float = None) -> Any:
        """
        Get the result of a specific task.
        """
        if task_id not in self.results:
            raise ValueError(f"Task {task_id} not found")
        
        task_info = self.results[task_id]
        future = task_info['future']
        
        try:
            result = future.result(timeout=timeout)
            task_info['status'] = 'completed'
            task_info['result'] = result
            return result
        except Exception as e:
            task_info['status'] = 'failed'
            task_info['error'] = str(e)
            raise e
    
    def wait_for_all_tasks(self, timeout: float = None) -> Dict[str, Any]:
        """
        Wait for all pending tasks to complete.
        Returns a dictionary of task results.
        """
        pending_tasks = [
            task_id for task_id, info in self.results.items() 
            if info['status'] == 'pending'
        ]
        
        results = {}
        for task_id in pending_tasks:
            try:
                result = self.get_task_result(task_id, timeout=timeout)
                results[task_id] = result
            except Exception as e:
                results[task_id] = {'error': str(e)}
        
        return results
    
    def get_task_status(self, task_id: str) -> str:
        """
        Get the current status of a task.
        """
        if task_id not in self.results:
            return 'not_found'
        
        task_info = self.results[task_id]
        if task_info['status'] == 'pending':
            if task_info['future'].done():
                if task_info['future'].exception():
                    task_info['status'] = 'failed'
                    task_info['error'] = str(task_info['future'].exception())
                else:
                    task_info['status'] = 'completed'
                    task_info['result'] = task_info['future'].result()
        
        return task_info['status']
    
    def cancel_task(self, task_id: str) -> bool:
        """
        Cancel a pending task.
        """
        if task_id not in self.results:
            return False
        
        task_info = self.results[task_id]
        if task_info['status'] == 'pending':
            cancelled = task_info['future'].cancel()
            if cancelled:
                task_info['status'] = 'cancelled'
                logger.info(f"Task {task_id} cancelled")
            return cancelled
        
        return False
    
    def start(self):
        """
        Start the worker pool.
        """
        if not self.is_running:
            self.is_running = True
            logger.info("Worker pool started")
    
    def stop(self, wait: bool = True):
        """
        Stop the worker pool and wait for tasks to complete if requested.
        """
        if self.is_running:
            self.is_running = False
            self.executor.shutdown(wait=wait)
            logger.info("Worker pool stopped")
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get statistics about the worker pool.
        """
        pending_count = sum(1 for info in self.results.values() if info['status'] == 'pending')
        completed_count = sum(1 for info in self.results.values() if info['status'] == 'completed')
        failed_count = sum(1 for info in self.results.values() if info['status'] == 'failed')
        cancelled_count = sum(1 for info in self.results.values() if info['status'] == 'cancelled')
        
        return {
            'total_tasks': len(self.results),
            'pending': pending_count,
            'completed': completed_count,
            'failed': failed_count,
            'cancelled': cancelled_count,
            'max_workers': self.max_workers,
            'is_running': self.is_running
        }
    
    def clear_completed_tasks(self):
        """
        Remove completed, failed, and cancelled tasks from memory.
        """
        completed_task_ids = [
            task_id for task_id, info in self.results.items()
            if info['status'] in ['completed', 'failed', 'cancelled']
        ]
        
        for task_id in completed_task_ids:
            del self.results[task_id]
        
        logger.info(f"Cleared {len(completed_task_ids)} completed tasks")
    
    def auto_cleanup(self, max_completed_tasks: int = 100):
        """
        Automatically cleanup completed tasks if they exceed a threshold.
        
        Args:
            max_completed_tasks: Maximum number of completed tasks to keep in memory
        """
        completed_count = sum(1 for info in self.results.values() 
                            if info['status'] in ['completed', 'failed', 'cancelled'])
        
        if completed_count > max_completed_tasks:
            logger.info(f"Auto-cleanup triggered: {completed_count} completed tasks exceed threshold {max_completed_tasks}")
            self.clear_completed_tasks()
    
    def get_memory_stats(self) -> Dict[str, Any]:
        """
        Get memory usage statistics for the worker pool.
        """
        import sys
        
        # Estimate memory usage of results dictionary
        estimated_memory = len(self.results) * 200  # Rough estimate: 200 bytes per task entry
        
        return {
            'results_dict_size': len(self.results),
            'estimated_memory_bytes': estimated_memory,
            'estimated_memory_mb': round(estimated_memory / (1024 * 1024), 2),
            'max_workers': self.max_workers,
            'is_running': self.is_running
        }
    
    def submit_offering_processing_task(self, offering_id: str, offering_data: Dict[str, Any], redis_config: Dict[str, Any]) -> str:
        """
        Submit a specific offering processing task to the worker pool.
        
        Args:
            offering_id: The offering ID to process
            offering_data: The offering data from DLT
            redis_config: Redis configuration
            
        Returns:
            Task ID for tracking
        """
        from utils import OfferingProcessor
        
        def process_single_offering(off_id: str, off_data: Dict[str, Any], redis_cfg: Dict[str, Any]) -> bool:
            """Worker function to process a single offering."""
            try:
                processor = OfferingProcessor(redis_cfg)
                return processor.process_offering(off_id, off_data)
            except Exception as e:
                logger.error(f"Error processing offering {off_id}: {e}")
                return False
        
        return self.submit_task(process_single_offering, offering_id, offering_data, redis_config)
    
    def submit_bulk_offering_processing(self, offerings: List[Dict[str, Any]], redis_config: Dict[str, Any]) -> List[str]:
        """
        Submit multiple offering processing tasks to the worker pool.
        
        Args:
            offerings: List of offering data dictionaries
            redis_config: Redis configuration
            
        Returns:
            List of task IDs for tracking
        """
        task_ids = []
        
        for offering_data in offerings:
            offering_id = offering_data[0]
            if offering_id:
                task_id = self.submit_offering_processing_task(offering_id, offering_data[1], redis_config)
                task_ids.append(task_id)
        
        logger.info(f"Submitted {len(task_ids)} offering processing tasks")
        return task_ids
    
    def __enter__(self):
        """Context manager entry."""
        self.start()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.stop(wait=True)
