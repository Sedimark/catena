import json
from typing import List, Dict, Any

import redis
import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from config import DLT_BASE_URL, REDIS_HOST, REDIS_PORT, REDIS_DB, REDIS_NODES_KEY
from utils.redis_store import get_kv_client


def _get_redis_client(host: str, port: int, db: int):
    return get_kv_client(host, port, db)


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=8), reraise=True)
def _fetch_nodes_from_dlt() -> List[Dict[str, Any]]:
    url = f"{DLT_BASE_URL}/nodes"
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    nodes = resp.json()
    # Normalize to list of {id, address}
    normalized: List[Dict[str, Any]] = []
    for n in nodes:
        node_id = n.get("id") or n.get("nodeId") or n.get("did")
        address = n.get("address") or n.get("url") or n.get("endpoint")
        if node_id and address:
            normalized.append({"id": node_id, "address": address})
    return normalized


def seed_placeholder_nodes(client: redis.Redis, key: str) -> List[Dict[str, Any]]:
    placeholder = [
        {"id": "node-1", "address": "http://localhost:3030"},
        {"id": "node-2", "address": "http://localhost:3031"},
        {"id": "node-3", "address": "http://localhost:3032"},
    ]
    client.set(key, json.dumps(placeholder))
    return placeholder


def fetch_and_store_nodes(redis_config: Dict[str, Any] | None = None) -> List[Dict[str, Any]]:
    if redis_config is None:
        redis_config = {"host": REDIS_HOST, "port": REDIS_PORT, "db": REDIS_DB, "key": REDIS_NODES_KEY}
    client = _get_redis_client(redis_config["host"], redis_config["port"], redis_config["db"])
    try:
        nodes = _fetch_nodes_from_dlt()
        if not nodes:
            nodes = seed_placeholder_nodes(client, redis_config["key"])
        else:
            client.set(redis_config["key"], json.dumps(nodes))
        return nodes
    except Exception:
        return seed_placeholder_nodes(client, redis_config["key"])


def get_node_list(redis_config: Dict[str, Any] | None = None) -> List[Dict[str, Any]]:
    if redis_config is None:
        redis_config = {"host": REDIS_HOST, "port": REDIS_PORT, "db": REDIS_DB, "key": REDIS_NODES_KEY}
    client = _get_redis_client(redis_config["host"], redis_config["port"], redis_config["db"])
    data = client.get(redis_config["key"])
    if data:
        try:
            return json.loads(data)
        except Exception:
            pass
    return fetch_and_store_nodes(redis_config)