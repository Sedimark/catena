import asyncio
import aiohttp
import logging
from flask import Flask, request, jsonify
from utils.dlt_comm.get_nodes import get_node_list
from config import REDIS_CONFIG

# no need for a sparql engine. bruteforce the way to go haha

app = Flask(__name__)
logger = logging.getLogger(__name__)

async def fetch_sparql(session, node, query):
    node_url = f"{node['node_url']}/sparql"
    payload = {"query": query}
    headers = {"Accept": "application/sparql-results+json"} # test this bit

    try:
        async with session.post(node_url, json=payload, headers=headers, timeout=10) as resp:
            if resp.status != 200:
                logger.warning(f"Node {node['owner']} returned status {resp.status}")
                return []
            result = await resp.json()
            return result.get("results", {}).get("bindings", [])
    except Exception as e:
        logger.warning(f"Failed to query node {node['owner']}: {e}")
        return []

async def federated_query(nodes, query):
    """
    Run the SPARQL query across all nodes asynchronously.
    Returns combined bindings.
    """
    results = []
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_sparql(session, node, query) for node in nodes]
        responses = await asyncio.gather(*tasks)
        for node_bindings in responses:
            results.extend(node_bindings)
    return results


@app.route("/sparql", methods=["POST"])
def federated_sparql():
    data = request.get_json()
    query = data.get("query")
    if not query:
        return jsonify({"error": "No query provided"}), 400

    # Get node list
    nodes = get_node_list(REDIS_CONFIG)
    if not nodes:
        return jsonify({"error": "No nodes available"}), 500

    # Run async queries
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    combined_bindings = loop.run_until_complete(federated_query(nodes, query))
    loop.close()

    return jsonify({
        "head": {"vars": query_vars(query)},
        "results": {"bindings": combined_bindings}
    })


def query_vars(sparql_query):
    import re
    match = re.search(r"SELECT\s+(DISTINCT\s+)?(.*?)\s+WHERE", sparql_query, re.IGNORECASE | re.DOTALL)
    if match:
        vars_str = match.group(2)
        return [v.strip().lstrip("?") for v in vars_str.split()]
    return []

# if __name__ == "__main__":
#     app.run(host="0.0.0.0", port=3030) #TODO: Remember to change the hard coded port


