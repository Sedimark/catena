import json
import redis
import requests
import logging
import urllib.parse
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
                    # node_url = f"{base_url}:3030/catalogue"

                    # The following block checks if the node already exists in the database
                    # If it does, it updates the node_url
                    # If it does not, it adds the node to the database
                    node_exists = redis_client.get(f"node:{offering_id}")
                    if node_exists:
                        logger.info(f"Node {offering_id} already exists in the database")
                    else:
                        # base_url = description_uri.split(':')[0] + ':' + description_uri.split(':')[1].split('/')[0]
                        parsed_uri = urllib.parse.urlparse(description_uri)
                        base_url = f"{parsed_uri.scheme}://{parsed_uri.netloc}"
                        if len(base_url.split(":")) > 2:
                            base_url = base_url.split(":")[0] + ":" + base_url.split(":")[1]
                        node_url = f"{base_url}:3030/catalogue"
                        # logger.info(f"Node URL: {node_url}") # debug log

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
                        redis_client.sadd('all_nodes', offering_id)
                    
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
            logger.info(f"Nodes data: {nodes_data}") # debug log
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
    This function retrieves the basic offering metadata that will be processed separately.
    """
    try:
        # Get all offering IDs from DLT
        offerings_url = f"{DLT_BASE_URL}/offerings"
        response = requests.get(offerings_url)
        response.raise_for_status()
        offerings_data = response.json()
        
        offering_ids = offerings_data.get('addresses', [])
        offerings_meta = []
        
        for offering_id in offering_ids:
            try:
                # Get offering metadata one by one basis
                offering_url = f"{DLT_BASE_URL}/offerings/{offering_id}"
                # logger.info(f"DLT_URL: {DLT_BASE_URL}") # debug log
                offering_response = requests.get(offering_url)
                offering_response.raise_for_status()
                offering = offering_response.json()
                offerings_meta.append(offering)
                
            except Exception as e:
                logger.error(f"Error fetching offering {offering_id}: {e}")
                continue
        
        return [offering_ids, offerings_meta]
        
    except Exception as e:
        logger.error(f"Error getting offerings for processing: {e}")
        return []
