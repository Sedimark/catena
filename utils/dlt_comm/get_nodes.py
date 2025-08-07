import json
import redis
import requests
from config import DLT_HOST

def fetch_and_store_nodes(redis_config):
    """
    Fetches node identities from DLT_HOST and their info, stores in Redis.
    """
    redis_client = redis.Redis(host=redis_config['host'], port=redis_config['port'], db=redis_config['db'], decode_responses=True)
    identities_url = f"http://{DLT_HOST}:{DLT_PORT}/api/identities"
    try:
        resp = requests.get(identities_url)
        resp.raise_for_status()
        node_ids = resp.json()
    except Exception as e:
        print(f"Failed to fetch identities: {e}")
        return None

    node_info_list = []
    for node_id in node_ids:
        dids_url = f"http://{DLT_HOST}:{DLT_PORT}/api/dids?id={node_id}"
        try:
            dids_resp = requests.get(dids_url)
            dids_resp.raise_for_status()
            node_info = dids_resp.json()
            node_info_list.append(node_info)
        except Exception as e:
            print(f"Failed to fetch info for node {node_id}: {e}")
            continue

    # Store in Redis as JSON
    redis_client.set(redis_config['key'], json.dumps(node_info_list))
    return node_info_list

def get_node_list(redis_config):
    """
    Retrieves a list of nodes from Redis. If not present, fetches and stores them first.
    """
    redis_client = redis.Redis(host=redis_config['host'], port=redis_config['port'], db=redis_config['db'], decode_responses=True)
    data = redis_client.get(redis_config['key'])
    if data:
        return json.loads(data)
    else:
        return fetch_and_store_nodes(redis_config)