import time
import logging
import requests
import threading
from typing import Dict, List, Any
from config import NODE_GRACE_PERIOD, NODE_HEALTH_CHECK_INTERVAL, NODE_TIMEOUT
from utils.dlt_comm.get_nodes import get_node_list
from utils.hash_ring.consistent_hash import ConsistentHashRing

logger = logging.getLogger(__name__)

class NodeHealthChecker:
    """
    Monitors node health and manages node failures with grace periods.
    """
    
    def __init__(self, redis_config: Dict[str, Any], grace_period: int = NODE_GRACE_PERIOD):
        self.redis_config = redis_config
        self.grace_period = grace_period  # in seconds
        self.node_failures = {}
        self.hash_ring = ConsistentHashRing(redis_config)
        self.health_check_interval = NODE_HEALTH_CHECK_INTERVAL  # in seconds
        self.node_timeout = NODE_TIMEOUT  # in seconds
        self.node_operation_lock = threading.Lock()
        
    def check_node_health(self, node_info: Dict[str, Any]) -> bool:
        """
        Check if a specific node is healthy by testing its catalogue endpoint.
        """
        # Test catalogue endpoint
        try:
            test_url = f"{node_info['node_url']}/test"
            response = requests.get(test_url, timeout=self.node_timeout)
            return response.status_code == 200
        except Exception as e:
            logger.warning(f"Node health check failed for {node_info.get('owner', 'unknown')}: {e}")
            return False
    
    def get_healthy_nodes(self) -> List[Dict[str, Any]]:
        """
        Get list of currently healthy nodes, considering grace periods.
        """
        # Get healthy nodes and manage unhealthy ondes, redistribute hash rings
        try:
            all_nodes = get_node_list(self.redis_config)
            if not all_nodes:
                return []
            
            healthy_nodes = []
            current_time = time.time()
            
            for node in all_nodes:
                node_id = node['owner']
                
                if self.check_node_health(node):
                    if node_id in self.node_failures:
                        del self.node_failures[node_id]
                        logger.info(f"Node {node_id} recovered")
                    
                    if node.get('status') != 'healthy':
                        node['status'] = 'healthy'
                        self.hash_ring.update_node_status(node_id, 'healthy')
                    
                    healthy_nodes.append(node)
                else:
                    if node_id not in self.node_failures:
                        self.node_failures[node_id] = current_time
                        logger.warning(f"Node {node_id} is down, starting grace period")
                    
                    failure_time = self.node_failures[node_id]
                    if current_time - failure_time >= self.grace_period:
                        with self.node_operation_lock:
                            if node.get('status') != 'unhealthy':
                                node['status'] = 'unhealthy'
                                self.hash_ring.update_node_status(node_id, 'unhealthy')
                                logger.error(f"Node {node_id} grace period expired, marking as unhealthy")
                            
                            self.hash_ring.redistribute_offerings(node_id, self.redis_config)
                            
                            self.hash_ring.remove_node(node_id)
                            logger.info(f"Node {node_id} removed from hash ring and offerings redistributed")
                    else:
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
        
        # Update hash ring with any newly discovered nodes
        try:
            healthy_nodes = self.get_healthy_nodes()
            logger.info(f"Found {len(healthy_nodes)} healthy nodes")
            
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

        # Initiate monitoring loop 
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
