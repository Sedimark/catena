import time
import logging
import requests
import threading
from typing import Dict, List, Any
from config import NODE_HEALTH_CHECK_INTERVAL
from utils.dlt_comm.get_nodes import get_node_list
from utils.hash_ring.consistent_hash import ConsistentHashRing

logger = logging.getLogger(__name__)

class NodeHealthChecker:
    """
    Monitors node health and manages node failures with grace periods.
    """
    
    def __init__(self, redis_config: Dict[str, Any], grace_period: int = 60):
        self.redis_config = redis_config
        self.grace_period = grace_period  # seconds
        self.node_failures = {}  # Track failure timestamps
        self.hash_ring = ConsistentHashRing(redis_config)
        self.health_check_interval = NODE_HEALTH_CHECK_INTERVAL  # seconds
        self.node_timeout = 10  # seconds for health check timeout
        self.node_operation_lock = threading.Lock()  # Prevent race conditions
        
    def check_node_health(self, node_info: Dict[str, Any]) -> bool:
        """
        Check if a specific node is healthy by testing its catalogue endpoint.
        """
        try:
            # Test the catalogue endpoint
            test_url = f"{node_info['node_url']}/test"
            response = requests.get(test_url, timeout=self.node_timeout)
            return response.status_code == 200
        except Exception as e:
            logger.warning(f"Node health check failed for {node_info.get('id', 'unknown')}: {e}")
            return False
    
    def get_healthy_nodes(self) -> List[Dict[str, Any]]:
        """
        Get list of currently healthy nodes, considering grace periods.
        """
        try:
            all_nodes = get_node_list(self.redis_config)
            if not all_nodes:
                return []
            
            healthy_nodes = []
            current_time = time.time()
            
            for node in all_nodes:
                node_id = node['id']
                
                if self.check_node_health(node):
                    # Node is healthy, clear any failure records
                    if node_id in self.node_failures:
                        del self.node_failures[node_id]
                        logger.info(f"Node {node_id} recovered")
                    
                    # Update hash ring if node was previously unhealthy
                    if node.get('status') != 'healthy':
                        node['status'] = 'healthy'
                        self.hash_ring.update_node_status(node_id, 'healthy')
                    
                    healthy_nodes.append(node)
                else:
                    # Node is down, record failure time
                    if node_id not in self.node_failures:
                        self.node_failures[node_id] = current_time
                        logger.warning(f"Node {node_id} is down, starting grace period")
                    
                    # Check if grace period has expired
                    failure_time = self.node_failures[node_id]
                    if current_time - failure_time >= self.grace_period:
                        # Use lock to make operations atomic
                        with self.node_operation_lock:
                            # Grace period expired, mark as unhealthy
                            if node.get('status') != 'unhealthy':
                                node['status'] = 'unhealthy'
                                self.hash_ring.update_node_status(node_id, 'unhealthy')
                                logger.error(f"Node {node_id} grace period expired, marking as unhealthy")
                            
                            # Redistribute offerings from this node
                            self.hash_ring.redistribute_offerings(node_id, self.redis_config)
                            
                            # Remove from hash ring
                            self.hash_ring.remove_node(node_id)
                            logger.info(f"Node {node_id} removed from hash ring and offerings redistributed")
                    else:
                        # Still in grace period, keep as potentially healthy
                        remaining_grace = self.grace_period - (current_time - failure_time)
                        logger.info(f"Node {node_id} still in grace period ({remaining_grace:.1f}s remaining)")
                        healthy_nodes.append(node)
            
            return healthy_nodes
            
        except Exception as e:
            logger.error(f"Error getting healthy nodes: {e}")
            return []
    
    def run_health_check_cycle(self):
        """
        Run one complete health check cycle.
        """
        logger.info("Starting health check cycle")
        
        try:
            healthy_nodes = self.get_healthy_nodes()
            logger.info(f"Found {len(healthy_nodes)} healthy nodes")
            
            # Update hash ring with any newly discovered nodes
            for node in healthy_nodes:
                if node['status'] == 'healthy':
                    self.hash_ring.add_node(node)
            
        except Exception as e:
            logger.error(f"Error in health check cycle: {e}")
    
    def start_monitoring(self):
        """
        Start continuous monitoring of nodes.
        """
        logger.info("Starting node health monitoring")
        
        while True:
            try:
                self.run_health_check_cycle()
                time.sleep(self.health_check_interval)
            except KeyboardInterrupt:
                logger.info("Stopping node health monitoring")
                break
            except Exception as e:
                logger.error(f"Unexpected error in monitoring loop: {e}")
                time.sleep(self.health_check_interval)
