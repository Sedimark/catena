import threading
import time
import requests
import logging
from typing import List, Dict

logger = logging.getLogger(__name__)

class NodeMonitor:
    def __init__(self, node_list_func, redis_config, check_interval=60):
        """
        node_list_func: function to get the current list of nodes (should accept redis_config)
        redis_config: dict with Redis connection info
        check_interval: seconds between health checks
        """
        self.node_list_func = node_list_func
        self.redis_config = redis_config
        self.check_interval = check_interval
        self.active_nodes = []
        self.down_nodes = []
        self._stop_event = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self):
        logger.info("Starting NodeMonitor thread.")
        self._thread.start()

    def stop(self):
        logger.info("Stopping NodeMonitor thread.")
        self._stop_event.set()
        self._thread.join()

    def _run(self):
        while not self._stop_event.is_set():
            self.check_nodes()
            time.sleep(self.check_interval)

    def check_nodes(self):
        nodes = self.node_list_func(self.redis_config)
        active = []
        down = []
        for node in nodes:
            node_url = node.get('url') or node.get('endpoint')
            if not node_url:
                continue
            try:
                resp = requests.get(node_url, timeout=3)
                if resp.status_code == 200:
                    active.append(node)
                else:
                    down.append(node)
            except Exception:
                down.append(node)
        self.active_nodes = active
        self.down_nodes = down
        logger.info(f"Active nodes: {len(active)}, Down nodes: {len(down)}")
        if down:
            self.handle_failover(down, active)

    def handle_failover(self, down_nodes: List[Dict], active_nodes: List[Dict]):
        """
        Placeholder for failover and data redistribution logic.
        """
        logger.warning(f"Failover triggered for {len(down_nodes)} down nodes. Redistributing data...")
        # TODO: Implement data reassignment and redistribution among active nodes