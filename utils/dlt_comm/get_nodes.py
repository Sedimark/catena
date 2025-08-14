import json
import redis
import requests
import logging
from typing import Dict, List, Any
from config import DLT_BASE_URL

logger = logging.getLogger(__name__)

def discover_and_store_nodes(redis_config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Discover nodes by fetching offerings from DLT and extracting node addresses.
    Stores nodes in Redis and returns the list of discovered nodes.
    """
    redis_client = redis.Redis(
        host=redis_config['host'], 
        port=redis_config['port'], 
        db=redis_config['db'], 
        decode_responses=True
    )
    
    try:
        # Get all offering IDs from DLT
        offerings_url = f"{DLT_BASE_URL}/offerings"
        response = requests.get(offerings_url)
        response.raise_for_status()
        offerings_data = response.json()
        
        offering_ids = offerings_data.get('addresses', [])
        discovered_nodes = []
        
        for offering_id in offering_ids:
            try:
                # Get offering details
                offering_url = f"{DLT_BASE_URL}/offerings/{offering_id}"
                offering_response = requests.get(offering_url)
                offering_response.raise_for_status()
                offering = offering_response.json()
                
                # Extract node address from descriptionUri
                description_uri = offering.get('descriptionUri', '')
                if description_uri:
                    # Extract base address (remove port and path, add 3030)
                    base_url = description_uri.split(':')[0] + ':' + description_uri.split(':')[1].split('/')[0]
                    node_url = f"{base_url}:3030/catalogue"
                    
                    node_info = {
                        'id': offering_id,
                        'address': base_url,
                        'node_url': node_url,
                        'owner': offering.get('owner'),
                        'name': offering.get('name'),
                        'status': 'healthy'
                    }
                    
                    discovered_nodes.append(node_info)
                    
                    # Store individual node in Redis
                    redis_client.hset(f"node:{offering_id}", mapping=node_info)
                    
            except Exception as e:
                logger.error(f"Error processing offering {offering_id}: {e}")
                continue
        
        # Store all nodes list in Redis
        redis_client.set('all_nodes', json.dumps(discovered_nodes))
        
        return discovered_nodes
        
    except Exception as e:
        logger.error(f"Error discovering nodes: {e}")
        return []

def get_node_list(redis_config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Retrieve list of nodes from Redis.
    """
    redis_client = redis.Redis(
        host=redis_config['host'], 
        port=redis_config['port'], 
        db=redis_config['db'], 
        decode_responses=True
    )
    
    try:
        nodes_data = redis_client.get('all_nodes')
        if nodes_data:
            return json.loads(nodes_data)
        else:
            # If no nodes in Redis, discover them
            return discover_and_store_nodes(redis_config)
    except Exception as e:
        logger.error(f"Error retrieving nodes: {e}")
        return []

def get_offerings_for_processing(redis_config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Get all offerings from DLT for processing.
    This function retrieves the basic offering data that will be processed separately.
    """
    try:
        # Get all offering IDs from DLT
        offerings_url = f"{DLT_BASE_URL}/offerings"
        response = requests.get(offerings_url)
        response.raise_for_status()
        offerings_data = response.json()
        
        offering_ids = offerings_data.get('addresses', [])
        offerings = []
        
        for offering_id in offering_ids:
            try:
                # Get offering details
                offering_url = f"{DLT_BASE_URL}/offerings/{offering_id}"
                # logger.info(f"DLT_URL: {DLT_BASE_URL}") # debug log
                offering_response = requests.get(offering_url)
                offering_response.raise_for_status()
                offering = offering_response.json()
                offerings.append(offering)
                
            except Exception as e:
                logger.error(f"Error fetching offering {offering_id}: {e}")
                continue
        
        return offerings
        
    except Exception as e:
        logger.error(f"Error getting offerings for processing: {e}")
        return []