import redis
import requests
import logging
import urllib.parse
from typing import Dict, List, Any
from config import DLT_BASE_URL, BASELINE_INFRA

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
    
    # Get and parse offeringURIs for catalogue endpoints
    try:
        logger.info(f"Getting and parsing offeringURIs for catalogue endpoints")
        offerings_url = f"{DLT_BASE_URL}/offerings"
        response = requests.get(offerings_url)
        response.raise_for_status()
        offerings_data = response.json()
        
        offering_ids = offerings_data.get('addresses', [])
        discovered_nodes = []
        
        # Fetch offering metadata and parse descriptionURI for catalogue endpoints
        if not BASELINE_INFRA:
            for offering_id in offering_ids:
                try:
                    offering_url = f"{DLT_BASE_URL}/offerings/{offering_id}"
                    offering_response = requests.get(offering_url)
                    offering_response.raise_for_status()
                    offering_meta = offering_response.json()
                    
                    description_uri = offering_meta.get('descriptionUri', '')
                    offering_owner = offering_meta.get('owner', '')
                    if description_uri:
                        # Check if node already exists in Redis
                        node_exists = redis_client.hgetall(f"node:{offering_owner}")
                        if node_exists:
                            logger.info(f"Node {offering_owner} already exists in the database")
                        else:
                            parsed_uri = urllib.parse.urlparse(description_uri)
                            base_url = f"{parsed_uri.scheme}://{parsed_uri.netloc}"
                            if len(base_url.split(":")) > 2:
                                base_url = base_url.split(":")[0] + ":" + base_url.split(":")[1]
                            
                            # TODO: Add a step for Self Description call to determine unique catalogue endpoints
                            node_url = f"{base_url}:3030/catalogue"
                            # TODO: Add a health check step for the catalogue endpoint

                            node_info = {
                                'address': base_url,
                                'node_url': node_url,
                                'owner': offering_meta.get('owner'),
                                'name': offering_meta.get('name'),
                                'status': 'healthy'
                            }
                                
                            discovered_nodes.append(node_info)
                            
                            # Store individual node in Redis
                            redis_client.hset(f"node:{offering_owner}", mapping=node_info)
                            redis_client.sadd('all_nodes', offering_owner)
                        
                except Exception as e:
                    logger.error(f"Error fetching and parsing catalogue endpoint information for offering {offering_id}: {e}")
                    continue

        # Baseline Infrastructure Implementation
        else:
            import json

            with open('catalogue_list.json', 'r') as f:
                nodes_data = json.load(f)

            node_info = {}

            for node_name, node_data in nodes_data.items():
                offering_owner = node_data['id']
                node_info = {
                    'address': node_data['base_url'],
                    'node_url': node_data['catalogue_url'],
                    'owner': node_data['id'],
                    'name': node_name,
                    'status': 'healthy'
                }
            
                discovered_nodes.append(node_info)
                            
                redis_client.hset(f"node:{offering_owner}", mapping=node_info)
                redis_client.sadd('all_nodes', offering_owner) 
        
        # Ensure all_nodes is of type SET
        if redis_client.exists("all_nodes") and redis_client.type("all_nodes") != "set":
            redis_client.delete("all_nodes")

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
        nodes_data = redis_client.smembers('all_nodes')
        if nodes_data:
            all_nodes = []
            for node_id in nodes_data:
                all_nodes.append(redis_client.hgetall(f"node:{node_id}"))

            return all_nodes
        else:
            logger.info(f"No nodes found in Redis, discovering and storing nodes")
            return discover_and_store_nodes(redis_config)
    except Exception as e:
        logger.error(f"Error retrieving nodes: {e}")
        return []

def get_offerings_meta_for_processing() -> List[Dict[str, Any]]:
    """
    Get all offerings from DLT for processing.
    This function retrieves the basic offering metadata that will be processed separately.
    """
    faulty_offerings = []

    # Get offering IDs and metadata whenever called
    try:
        offerings_url = f"{DLT_BASE_URL}/offerings"
        response = requests.get(offerings_url)
        response.raise_for_status()
        offerings_data = response.json()
        
        offering_ids = offerings_data.get('addresses', [])
        offerings_meta = []
        
        for offering_id in offering_ids:
            try:
                offering_url = f"{DLT_BASE_URL}/offerings/{offering_id}"
                offering_response = requests.get(offering_url)
                offering_response.raise_for_status()
                offering = offering_response.json()
                offerings_meta.append(offering)
                
            except Exception as e:
                logger.error(f"Error fetching offering metadata for {offering_id}: {e}")
                faulty_offerings.append(offering_id)
                continue
        
        if faulty_offerings:
            for offering_id in faulty_offerings:
                logger.info(f"Removing faulty offering {offering_id} from offerings for processing")
                offering_ids.remove(offering_id)

        return [offering_ids, offerings_meta]
        
    except Exception as e:
        logger.error(f"Error getting offerings for processing: {e}")
        return []
