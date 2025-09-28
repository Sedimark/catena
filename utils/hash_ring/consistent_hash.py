import hashlib
import json
import redis
import logging
from typing import Dict, List, Any, Optional
import requests

logger = logging.getLogger(__name__)

class ConsistentHashRing:
    """
    Consistent hashing ring implementation with virtual nodes for better distribution.
    """
    
    def __init__(self, redis_config: Dict[str, Any], virtual_nodes: int = 150):
        self.redis_config = redis_config
        self.virtual_nodes = virtual_nodes
        self.ring = {}
        self.sorted_keys = []
        self.redis_client = redis.Redis(
            host=redis_config['host'],
            port=redis_config['port'],
            db=redis_config['db'],
            decode_responses=True,
            socket_connect_timeout=5,
            socket_timeout=5
        )
        self._load_from_redis()
    
    def _load_from_redis(self):
        """Load hash ring state from Redis if available."""
        try:
            ring_data = self.redis_client.get('hash_ring')
            if ring_data:
                ring_state = json.loads(ring_data)
                self.ring = ring_state.get('ring', {})
                self.sorted_keys = ring_state.get('sorted_keys', [])
                logger.info("Hash ring loaded from Redis")
            else:
                logger.info("No hash ring found in Redis, starting fresh")
            
            node_ids = self.redis_client.smembers('all_nodes')
            self.nodes = [self.redis_client.hgetall(f"node:{node_id}") for node_id in node_ids]

        except Exception as e:
            logger.error(f"Error loading hash ring from Redis: {e}")
    
    def _save_to_redis(self):
        """Save hash ring state to Redis."""
        try:
            ring_state = {
                'ring': self.ring,
                'sorted_keys': self.sorted_keys,
            }
            self.redis_client.set('hash_ring', json.dumps(ring_state))
        except Exception as e:
            logger.error(f"Error saving hash ring to Redis: {e}")
    
    def _hash(self, key: str) -> int:
        """Generate hash for a key."""
        return int(hashlib.md5(key.encode()).hexdigest(), 16)
    
    def add_node(self, node: Dict[str, Any]):
        """Add a node to the hash ring."""
        node_id = node['owner']

        if not self.redis_client.exists(f"node:{node_id}"):
            self.redis_client.hset(f"node:{node_id}", mapping=node)
            self.redis_client.sadd("all_nodes", node_id)

        if node_id not in [n['owner'] for n in self.nodes]:
            self.nodes.append(node)
            
            # Add virtual nodes
            for i in range(self.virtual_nodes):
                virtual_key = f"{node_id}-{i}"
                hash_value = self._hash(virtual_key)
                self.ring[hash_value] = node_id
                self.sorted_keys.append(hash_value)
            
            self.sorted_keys.sort()
            self._save_to_redis()
            logger.info(f"Added node {node_id} to hash ring")
    
    def remove_node(self, node_id: str):
        """Remove a node from the hash ring."""
        try:
            # Remove virtual nodes
            for i in range(self.virtual_nodes):
                virtual_key = f"{node_id}-{i}"
                hash_value = self._hash(virtual_key)
                if hash_value in self.ring:
                    del self.ring[hash_value]
                    try:
                        self.sorted_keys.remove(hash_value)
                    except ValueError:
                        pass
            
            # Remove from nodes list
            self.nodes = [n for n in self.nodes if n['owner'] != node_id]

            self.redis_client.srem("all_nodes", node_id)     # remove ID from index
            self.redis_client.delete(f"node:{node_id}")

            self._save_to_redis()
            logger.info(f"Removed node {node_id} from hash ring")

        except Exception as e:
            logger.error(f"Error removing node {node_id} from hash ring: {e}")

            # Try to recover by saving current state
            try:
                self._save_to_redis()
            except Exception as save_error:
                logger.error(f"Failed to save hash ring state after error: {save_error}")
    
    def get_node(self, key: str) -> Optional[Dict[str, Any]]:
        """Get the node responsible for a given key."""
        if not self.sorted_keys:
            return None
        
        hash_value = self._hash(key)
        
        # Find the first node with hash >= key hash
        for sorted_key in self.sorted_keys:
            if sorted_key >= hash_value:
                node_id = self.ring[sorted_key]
                return next((n for n in self.nodes if n['owner'] == node_id), None)
        
        # Wrap around to the first node
        first_key = self.sorted_keys[0]
        node_id = self.ring[first_key]
        return next((n for n in self.nodes if n['owner'] == node_id), None)
    
    def get_nodes_for_key(self, key: str, replica_count: int = 2) -> List[Dict[str, Any]]:
        """Get multiple nodes for replication (primary + replicas)."""
        if not self.sorted_keys:
            return []
        
        hash_value = self._hash(key)
        nodes = []
        seen_node_ids = set()
        
        # Find primary node
        for sorted_key in self.sorted_keys:
            if sorted_key >= hash_value:
                node_id = self.ring[sorted_key]
                if node_id not in seen_node_ids:
                    node = next((n for n in self.nodes if n['owner'] == node_id), None)
                    if node:
                        nodes.append(node)
                        seen_node_ids.add(node_id)
                        if len(nodes) >= replica_count:
                            break
        
        if len(nodes) < replica_count:
            for sorted_key in self.sorted_keys:
                node_id = self.ring[sorted_key]
                if node_id not in seen_node_ids:
                    node = next((n for n in self.nodes if n['owner'] == node_id), None)
                    if node:
                        nodes.append(node)
                        seen_node_ids.add(node_id)
                        if len(nodes) >= replica_count:
                            break
        
        return nodes[:replica_count]
    
    def redistribute_offerings(self, failed_node_id: str, redis_config: Dict[str, Any]):
        """Redistribute offerings from a failed node to healthy nodes."""
        try:
            redis_client = redis.Redis(
                host=redis_config['host'],
                port=redis_config['port'],
                db=redis_config['db'],
                decode_responses=True
            )
            
            # Get offerings assigned to the failed node
            failed_offerings = redis_client.smembers(f"node_offerings:{failed_node_id}")
            
            # Get offering data and store again
            for offering_id in failed_offerings:
                offering_data = redis_client.get(f"offering:{offering_id}")
                if offering_data:
                    offering = json.loads(offering_data)
                    
                    new_nodes = self.get_nodes_for_key(offering_id, replica_count=2)
                    
                    if new_nodes:
                        for node in new_nodes:
                            try:
                                response = requests.post(
                                    f"{node['node_url']}/manager",
                                    json=offering,
                                    headers={'Content-Type': 'application/ld+json'}
                                )
                                if response.status_code in [200, 201]:
                                    # Update Redis to reflect new assignment
                                    redis_client.sadd(f"node_offerings:{node['owner']}", offering_id)
                                    redis_client.delete(f"offering:{offering_id}")
                                    logger.info(f"Redistributed offering {offering_id} to node {node['owner']}")
                            except Exception as e:
                                logger.error(f"Error redistributing offering {offering_id} to node {node['owner']}: {e}")
            
            # Remove failed node's offerings from Redis
            redis_client.delete(f"node_offerings:{failed_node_id}")
            
        except Exception as e:
            logger.error(f"Error redistributing offerings: {e}")
    
    def update_node_status(self, node_id: str, status: str):
        """Update node status in the hash ring."""
        for node in self.nodes:
            if node['owner'] == node_id:
                node['status'] = status
                self._save_to_redis()
                break
