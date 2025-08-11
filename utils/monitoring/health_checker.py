"""
Health checking utilities for catalogue nodes.
"""

import requests
import logging
from typing import Dict, Any, List, Tuple

logger = logging.getLogger(__name__)


class HealthChecker:
    """Health checker for catalogue nodes."""
    
    def __init__(self, timeout: int = 3, health_endpoint: str = "/health"):
        """
        Initialize health checker.
        
        Args:
            timeout: Request timeout in seconds
            health_endpoint: Health check endpoint path
        """
        self.timeout = timeout
        self.health_endpoint = health_endpoint
    
    def check_node(self, node: Dict[str, Any]) -> Tuple[bool, str]:
        """
        Check health of a single node.
        
        Args:
            node: Node dictionary with address information
            
        Returns:
            Tuple of (is_healthy, error_message)
        """
        node_url = node.get('address') or node.get('url') or node.get('endpoint')
        if not node_url:
            return False, "No valid URL found"
        
        try:
            health_url = f"{node_url.rstrip('/')}{self.health_endpoint}"
            response = requests.get(health_url, timeout=self.timeout)
            
            if response.status_code == 200:
                return True, ""
            else:
                return False, f"HTTP {response.status_code}"
                
        except requests.exceptions.Timeout:
            return False, "Timeout"
        except requests.exceptions.ConnectionError:
            return False, "Connection failed"
        except Exception as e:
            return False, str(e)
    
    def check_nodes(self, nodes: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        Check health of multiple nodes.
        
        Args:
            nodes: List of node dictionaries
            
        Returns:
            Tuple of (healthy_nodes, unhealthy_nodes)
        """
        healthy = []
        unhealthy = []
        
        for node in nodes:
            is_healthy, error = self.check_node(node)
            if is_healthy:
                healthy.append(node)
            else:
                # Add error info to node for debugging
                node_with_error = node.copy()
                node_with_error['health_error'] = error
                unhealthy.append(node_with_error)
        
        logger.info(f"Health check complete: {len(healthy)} healthy, {len(unhealthy)} unhealthy")
        return healthy, unhealthy
