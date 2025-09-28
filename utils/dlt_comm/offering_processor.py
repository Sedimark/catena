import json
import requests
import logging
from typing import Dict, List, Any, Optional
from config import OFFERING_DESC_TIMEOUT, OFFERING_REPLICA_COUNT
from utils.hash_ring.consistent_hash import ConsistentHashRing
import time

logger = logging.getLogger(__name__)

class OfferingProcessor:
    """
    Handles the processing and distribution of offerings using consistent hashing.
    """
    
    def __init__(self, redis_config: Dict[str, Any]):
        self.redis_config = redis_config
        self.hash_ring = ConsistentHashRing(redis_config)
        self.redis_client = redis.Redis(
            host=redis_config['host'],
            port=redis_config['port'],
            db=redis_config['db'],
            decode_responses=True,
            socket_connect_timeout=5,
            socket_timeout=5
        )
    
    # Fetch and store offerings
    def process_offering(self, offering_id: str, offering_data: Dict[str, Any]) -> bool:
        """
        Process an offering: fetch from descriptionUri and distribute via consistent hashing.
        
        Args:
            offering_id: The offering ID
            offering_data: Basic offering data from DLT (contains descriptionUri)
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            description_uri = offering_data.get('descriptionUri')
            if not description_uri:
                logger.error(f"No descriptionUri found for offering {offering_id}")
                return False
            
            logger.info(f"Fetching offering from {description_uri}")
            response = requests.get(description_uri, timeout=OFFERING_DESC_TIMEOUT)
            response.raise_for_status()
            full_offering = response.json()
            
            target_nodes = self.hash_ring.get_nodes_for_key(offering_id, replica_count=OFFERING_REPLICA_COUNT)
            if not target_nodes:
                logger.error(f"No target nodes found for offering {offering_id}")
                return False
            
            successful_stores = 0
            for target_node in target_nodes:
                success = self._store_offering_in_node(target_node, full_offering, offering_id)
                if success:
                    successful_stores += 1
                    self._update_offering_assignment(offering_id, target_node['id'], full_offering)
                    logger.info(f"Successfully stored offering {offering_id} in node {target_node['id']}")
                else:
                    logger.error(f"Failed to store offering {offering_id} in node {target_node['id']}")
            
            if successful_stores > 0:
                logger.info(f"Offering {offering_id} stored in {successful_stores}/{len(target_nodes)} nodes")
                return True
            else:
                logger.error(f"Failed to store offering {offering_id} in any target nodes")
                return False
                
        except Exception as e:
            logger.error(f"Error processing offering {offering_id}: {e}")
            return False
    
    def _store_offering_in_node(self, target_node: Dict[str, Any], offering: Dict[str, Any], offering_id: str) -> bool:
        """
        Store the offering in the target node via POST request.
        
        Args:
            target_node: Node information including node_url
            offering: The full offering JSON-LD data
            offering_id: The offering ID for logging
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            manager_url = f"{target_node['node_url']}/manager"
            
            response = requests.post(
                manager_url,
                json=offering,
                headers={'Content-Type': 'application/ld+json'},
                timeout=30
            )
            
            if response.status_code in [200, 201]:
                logger.info(f"Offering {offering_id} stored successfully in node {target_node['id']}")
                return True
            else:
                logger.error(f"Failed to store offering {offering_id} in node {target_node['id']}: {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"Error storing offering {offering_id} in node {target_node['id']}: {e}")
            return False
    
    def _update_offering_assignment(self, offering_id: str, node_id: str, offering: Dict[str, Any]):
        """
        Update Redis to track which offerings are assigned to which nodes.
        
        Args:
            offering_id: The offering ID
            node_id: The node ID where the offering is stored
            offering: The offering data to store
        """
        max_retries = 3
        retry_delay = 1  # seconds
        
        for attempt in range(max_retries):
            try:
                # Use the pre-initialized Redis client
                redis_client = self.redis_client
                
                redis_client.ping()
                
                redis_client.set(f"offering:{offering_id}", json.dumps(offering))
                
                redis_client.sadd(f"node_offerings:{node_id}", offering_id)
                
                redis_client.set(f"offering_node:{offering_id}", node_id)
                
                logger.debug(f"Updated Redis assignment: offering {offering_id} -> node {node_id}")
                break
                
            except redis.ConnectionError as e:
                logger.error(f"Redis connection error (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    retry_delay *= 2
                else:
                    logger.error(f"Failed to update Redis after {max_retries} attempts")
            except Exception as e:
                logger.error(f"Error updating Redis assignment for offering {offering_id}: {e}")
                break
    
    def process_multiple_offerings(self, offerings: List[Dict[str, Any]]) -> Dict[str, bool]:
        """
        Process multiple offerings in batch.
        
        Args:
            offerings: List of offering data dictionaries
            
        Returns:
            Dict mapping offering_id to success status
        """
        results = {}
        
        for offering_data in offerings: # TODO: implement SHACL segregation of offerings
            offering_id = offering_data.get('name')
            if offering_id:
                success = self.process_offering(offering_id, offering_data)
                results[offering_id] = success
        
        return results
    
    def get_offering_status(self, offering_id: str) -> Optional[Dict[str, Any]]:
        """
        Get the current status and location of an offering.
        
        Args:
            offering_id: The offering ID
            
        Returns:
            Dict with offering status and node assignment, or None if not found
        """
        try:
            redis_client = self.redis_client
            
            node_id = redis_client.get(f"offering_node:{offering_id}")
            if not node_id:
                return None
            
            offering_data = redis_client.get(f"offering:{offering_id}")
            offering = json.loads(offering_data) if offering_data else None
            
            return {
                'offering_id': offering_id,
                'assigned_node': node_id,
                'offering_data': offering,
                'status': 'stored' if offering else 'missing'
            }
            
        except Exception as e:
            logger.error(f"Error getting status for offering {offering_id}: {e}")
            return None
