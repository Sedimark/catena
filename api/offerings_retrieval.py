from flask import Flask, request, jsonify
from typing import Dict, Any, List
import requests
from tenacity import retry, stop_after_attempt, wait_exponential
from cachetools import TTLCache
import time

from config import DLT_BASE_URL, DLT_OFFERINGS_PATH
from utils.dlt_comm.get_nodes import get_node_list

# Create the Flask app as a variable so it can be imported elsewhere
app = Flask(__name__)
_cache = TTLCache(maxsize=256, ttl=30)


@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint for Docker health checks."""
    return jsonify({
        "status": "healthy",
        "service": "catalogue-coordinator",
        "timestamp": time.time()
    })


def build_federated_query(sparql_query: str, node_endpoints: List[str]) -> List[Dict[str, Any]]:
    # Placeholder: send the same query to all nodes, return list of results
    results: List[Dict[str, Any]] = []
    headers = {"Content-Type": "application/sparql-query"}
    for endpoint in node_endpoints:
        try:
            resp = requests.post(f"{endpoint}/query", data=sparql_query, headers=headers, timeout=20)
            if resp.status_code in (200, 204):
                try:
                    results.append({"endpoint": endpoint, "data": resp.json()})
                except Exception:
                    results.append({"endpoint": endpoint, "data": resp.text})
            else:
                results.append({"endpoint": endpoint, "error": resp.status_code})
        except Exception as e:
            results.append({"endpoint": endpoint, "error": str(e)})
    return results


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=8))
def retrieve_offerings_by_id(offerings_id: str) -> Dict[str, Any]:
    cache_key = f"offerings:{offerings_id}"
    if cache_key in _cache:
        return _cache[cache_key]
    url = f"{DLT_BASE_URL}{DLT_OFFERINGS_PATH}"
    resp = requests.get(url, timeout=15)
    resp.raise_for_status()
    payload = resp.json()
    addresses = payload.get("addresses", [])
    # Placeholder: just return addresses filtered by offerings_id if matches
    filtered = [a for a in addresses if offerings_id in a]
    result = {"status": "success", "offerings_id": offerings_id, "addresses": filtered}
    _cache[cache_key] = result
    return result


@app.route('/offerings', methods=['POST'])
def get_offerings():
    data = request.get_json(force=True, silent=True) or {}
    offerings_id = data.get('offerings_id')
    if not offerings_id:
        return jsonify({
            "status": "error",
            "message": "'offerings_id' is required."
        }), 400
    result = retrieve_offerings_by_id(offerings_id)
    return jsonify(result)


@app.route('/sparql', methods=['POST'])
def federated_sparql():
    body = request.get_json(force=True, silent=True) or {}
    query = body.get("query")
    if not query:
        return jsonify({"status": "error", "message": "'query' is required"}), 400
    nodes = get_node_list(None)
    endpoints = [n.get("address") for n in nodes if n.get("address")]
    results = build_federated_query(query, endpoints)
    return jsonify({"status": "success", "results": results})

# Note: Do NOT run app.run() here. This file is meant to be imported and run from another file.