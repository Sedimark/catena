"""
Consistent hashing implementation using libketama algorithm.
"""

from typing import List, Dict, Any
from uhashring import HashRing


class KetamaRouter:
    """
    Consistent hashing router using libketama algorithm.
    
    Provides stable data distribution across nodes with support for
    multiple replicas and node weight balancing.
    """
    
    def __init__(self, nodes: List[Dict[str, Any]]):
        """
        Initialize router with list of nodes.
        
        Args:
            nodes: List of node dictionaries with 'id' and 'address' keys
        """
        # Build node map for quick lookups
        self.nodes_map = {n["id"]: n for n in nodes if n.get("id")}
        
        if not self.nodes_map:
            raise ValueError("No valid nodes provided")
        
        # Initialize hash ring
        self.ring = HashRing(nodes=list(self.nodes_map.keys()))
    
    def get_primary(self, key: str) -> Dict[str, Any]:
        """
        Get primary node for a given key.
        
        Args:
            key: The key to hash
            
        Returns:
            Node dictionary
        """
        node_id = self.ring.get_node(key)
        return self.nodes_map[node_id]
    
    def get_n(self, key: str, n: int) -> List[Dict[str, Any]]:
        """
        Get N unique nodes for a given key (for redundancy).
        
        Args:
            key: The key to hash
            n: Number of nodes to return
            
        Returns:
            List of node dictionaries
        """
        if n <= 0:
            return []
        
        selected: List[Dict[str, Any]] = []
        seen = set()
        
        # Get nodes from ring, ensuring uniqueness
        for node_id in self.ring.get_nodes(key):
            if node_id in seen:
                continue
            seen.add(node_id)
            selected.append(self.nodes_map[node_id])
            
            if len(selected) >= n:
                break
        
        return selected
    
    def add_node(self, node_id: str, node_info: Dict[str, Any]) -> None:
        """
        Add a new node to the ring.
        
        Args:
            node_id: Unique identifier for the node
            node_info: Node information dictionary
        """
        self.nodes_map[node_id] = node_info
        self.ring.add_node(node_id)
    
    def remove_node(self, node_id: str) -> None:
        """
        Remove a node from the ring.
        
        Args:
            node_id: Identifier of the node to remove
        """
        if node_id in self.nodes_map:
            del self.nodes_map[node_id]
            self.ring.remove_node(node_id)
    
    def get_all_nodes(self) -> List[Dict[str, Any]]:
        """
        Get all nodes in the ring.
        
        Returns:
            List of all node dictionaries
        """
        return list(self.nodes_map.values())
    
    def get_node_count(self) -> int:
        """
        Get total number of nodes.
        
        Returns:
            Number of nodes in the ring
        """
        return len(self.nodes_map)
