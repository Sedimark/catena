"""
Node monitoring service with failover handling.
"""

import threading
import time
import logging
from typing import List, Dict, Any, Callable

from utils.redis import get_redis_client
from utils.hashring import KetamaRouter
from utils.workers.offering_worker import fetch_listing, listing_to_sparql_insert, post_sparql
from .health_checker import HealthChecker

from config import (
    REDIS_HOST,
    REDIS_PORT,
    REDIS_DB,
    REDIS_DATA_KEY_PREFIX,
    REDUNDANCY_REPLICAS,
)

logger = logging.getLogger(__name__)


class NodeMonitor:
    """
    Monitors catalogue nodes and handles failover scenarios.
    
    Continuously checks node health and redistributes data when
    nodes become unavailable.
    """
    
    def __init__(
        self,
        node_list_func: Callable[[Dict[str, Any]], List[Dict[str, Any]]],
        redis_config: Dict[str, Any],
        check_interval: int = 60,
        health_timeout: int = 3,
    ):
        """
        Initialize node monitor.
        
        Args:
            node_list_func: Function to get current node list
            redis_config: Redis configuration dictionary
            check_interval: Seconds between health checks
            health_timeout: Health check timeout in seconds
        """
        self.node_list_func = node_list_func
        self.redis_config = redis_config
        self.check_interval = check_interval
        self.health_checker = HealthChecker(timeout=health_timeout)
        
        self.active_nodes: List[Dict[str, Any]] = []
        self.down_nodes: List[Dict[str, Any]] = []
        self._stop_event = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)
    
    def start(self):
        """Start the monitoring thread."""
        logger.info("Starting NodeMonitor thread.")
        self._thread.start()
    
    def stop(self):
        """Stop the monitoring thread."""
        logger.info("Stopping NodeMonitor thread.")
        self._stop_event.set()
        self._thread.join()
    
    def _run(self):
        """Main monitoring loop."""
        while not self._stop_event.is_set():
            self.check_nodes()
            time.sleep(self.check_interval)
    
    def check_nodes(self):
        """Check health of all nodes and handle changes."""
        nodes = self.node_list_func(self.redis_config)
        if not nodes:
            return
        
        # Use health checker to categorize nodes
        healthy, unhealthy = self.health_checker.check_nodes(nodes)
        
        # Check if we have node state changes
        if self._has_node_changes(healthy, unhealthy):
            self._handle_node_changes(healthy, unhealthy)
        
        self.active_nodes = healthy
        self.down_nodes = unhealthy
    
    def _has_node_changes(self, healthy: List[Dict[str, Any]], unhealthy: List[Dict[str, Any]]) -> bool:
        """Check if node states have changed since last check."""
        current_active_ids = {n.get('id') for n in healthy}
        current_down_ids = {n.get('id') for n in unhealthy}
        
        previous_active_ids = {n.get('id') for n in self.active_nodes}
        previous_down_ids = {n.get('id') for n in self.down_nodes}
        
        return (current_active_ids != previous_active_ids or 
                current_down_ids != previous_down_ids)
    
    def _handle_node_changes(self, healthy: List[Dict[str, Any]], unhealthy: List[Dict[str, Any]]):
        """Handle changes in node availability."""
        newly_down = [n for n in unhealthy if n.get('id') not in {dn.get('id') for dn in self.down_nodes}]
        
        if newly_down:
            logger.warning(f"New nodes down: {[n.get('id') for n in newly_down]}")
            self._handle_failover(newly_down, healthy)
    
    def _handle_failover(self, down_nodes: List[Dict[str, Any]], active_nodes: List[Dict[str, Any]]):
        """
        Handle failover by redistributing data from down nodes.
        
        Args:
            down_nodes: List of nodes that just went down
            active_nodes: List of currently healthy nodes
        """
        logger.warning(f"Failover triggered for {len(down_nodes)} down nodes. Redistributing data...")
        
        if not active_nodes:
            logger.error("No active nodes available for failover")
            return
        
        # Get Redis client
        client = get_redis_client(
            self.redis_config["host"],
            self.redis_config["port"],
            self.redis_config["db"]
        )
        
        # Build router from active nodes only
        try:
            router = KetamaRouter(active_nodes)
        except ValueError as e:
            logger.error(f"Failed to create router: {e}")
            return
        
        # Scan through listing pointers and redistribute affected data
        self._redistribute_affected_data(client, router, down_nodes)
    
    def _redistribute_affected_data(self, client, router: KetamaRouter, down_nodes: List[Dict[str, Any]]):
        """Redistribute data affected by node failures."""
        down_ids = {n.get('id') for n in down_nodes if n.get('id')}
        
        # Scan Redis for affected data
        cursor = 0
        pattern = f"{REDIS_DATA_KEY_PREFIX}*"
        
        while True:
            cursor, keys = client.scan(cursor=cursor, match=pattern, count=200)
            
            for key in keys:
                try:
                    self._redistribute_single_listing(client, router, key, down_ids)
                except Exception as e:
                    logger.error(f"Failed to redistribute listing {key}: {e}")
                    continue
            
            if cursor == 0:
                break
    
    def _redistribute_single_listing(self, client, router: KetamaRouter, key: str, down_ids: set):
        """Redistribute a single listing to new nodes."""
        data = client.get(key)
        if not data:
            return
        
        try:
            import json
            info = json.loads(data)
        except Exception:
            return
        
        listing_id = key.removeprefix(REDIS_DATA_KEY_PREFIX)
        old_targets = set(info.get('targets') or [])
        
        # If none of the old targets are down, skip
        if not (old_targets & down_ids):
            return
        
        # Compute new targets using active ring
        new_targets = router.get_n(listing_id, max(1, REDUNDANCY_REPLICAS))
        new_target_ids = [t['id'] for t in new_targets]
        
        # Re-fetch listing and redistribute
        listing_src = info.get('address')
        if not listing_src:
            return
        
        try:
            listing = fetch_listing(listing_src)
            sparql = listing_to_sparql_insert(listing)
            
            # Post to new targets
            for target in new_targets:
                success = post_sparql(target['address'], sparql)
                if success:
                    logger.info(f"Redistributed {listing_id} to {target['id']}")
                else:
                    logger.warning(f"Failed to redistribute {listing_id} to {target['id']}")
            
            # Update pointer in Redis
            client.set(key, json.dumps({
                "address": listing_src,
                "targets": new_target_ids
            }))
            
        except Exception as e:
            logger.error(f"Failed to redistribute listing {listing_id}: {e}")
    
    def get_active_nodes(self) -> List[Dict[str, Any]]:
        """Get list of currently active nodes."""
        return self.active_nodes.copy()
    
    def get_down_nodes(self) -> List[Dict[str, Any]]:
        """Get list of currently down nodes."""
        return self.down_nodes.copy()
    
    def get_node_status(self) -> Dict[str, Any]:
        """Get current node status summary."""
        return {
            "active_count": len(self.active_nodes),
            "down_count": len(self.down_nodes),
            "active_nodes": [n.get('id') for n in self.active_nodes],
            "down_nodes": [n.get('id') for n in self.down_nodes]
        }
