# import time
# import argparse
# import requests
# import json
# from typing import Dict, Any
# from rdflib import Graph
# from termcolor import colored
# import hashlib

# # A placeholder function to get all nodes 
# def get_all_nodes():
#     return {
#         0: "http://localhost:3030",
#         1: "http://localhost:3031",
#         2: "http://localhost:3032",
#         3: "http://localhost:3033",
#     }

# def verify_listing(listing: Dict[str, Any]) -> bool:
#     # TODO: Implement verification logic
#     return True

# def get_offering_id(listing):
#     offering_obj =  [obj for obj in listing if ':Offering' in obj.get('@type') and 'OfferingContract' not in obj.get('@type')]
#     if len(offering_obj) != 1:
#         raise Exception("Error parsing self listing.")
#     return offering_obj[0].get("@id").encode()

# def get_target_url(listing: Dict[str, Any]):
#     hash_value = int(hashlib.sha1(get_offering_id(listing)).hexdigest(), 16)
#     node_idx = hash_value % len(get_all_nodes())
#     return f"{get_all_nodes()[node_idx]}/catalogue/update"

# def get_sparql_query(listing: Dict[str, Any]):
#     g = Graph()
#     g.parse(data=json.dumps(listing), format='json-ld')
#     triples = g.serialize(format='nt')
#     return f"""
#         INSERT DATA {{
#             {triples}
#         }}
#         """

# def insert_listing(listing: Dict[str, Any]):
#     target_url = get_target_url(listing)
#     insert_query = get_sparql_query(listing)
#     headers = {"Content-Type": "application/sparql-update"}
#     response = requests.post(target_url, headers=headers, data=insert_query)
#     if response.status_code in [200, 204]:
#         print(colored("SUCCESS", "green"), f"Data stored at {target_url}")
#     else:
#         print(f"Failed to store data: {response.status_code}")

# def fetch_and_process_listing(source_url: str):
#     try:
#         response = requests.get(source_url)
#         response.raise_for_status()
#         data = response.json()
#         if not verify_listing(data):
#             print("Listing verification failed")
#             return
#         try:
#             insert_listing(data["@graph"])
#         except Exception as e:
#             print("Error in insert_listing:", e)
#     except Exception as e:
#         print("Error fetching or processing listing:", e)

# def main():
#     parser = argparse.ArgumentParser(description="Client to fetch JSON-LD listing and process it.")
#     parser.add_argument('--source-url', type=str, required=True, help='URL to fetch the JSON-LD object from')
#     parser.add_argument('--interval', type=int, default=60, help='Interval in seconds between fetches (default: 60)')
#     args = parser.parse_args()
#     while True:
#         fetch_and_process_listing(args.source_url)
#         time.sleep(args.interval)

# if __name__ == "__main__":
#     main()

# import argparse

import time
from multiprocessing import Process
from typing import Any, Dict, List
import logging
import os


from api import app
from config import HOST_ADDRESS, HOST_PORT

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s in %(module)s: %(message)s')
logger = logging.getLogger(__name__)

def node_list_setup():
    """
    Set up Redis connection variables for node list storage.
    """
    redis_config = {
        'host': os.getenv('REDIS_HOST', 'localhost'),
        'port': int(os.getenv('REDIS_PORT', 6379)),
        'db': int(os.getenv('REDIS_DB', 0)),
        'key': 'nodes',
    }
    return redis_config

def setup_server():
    logger.info(f"Starting Flask API server at {HOST_ADDRESS}:{HOST_PORT}")
    app.run(host=HOST_ADDRESS, port=HOST_PORT)


def main():
    """
    Catalogue Co-ordinator entry point.
    """

    # parser = argparse.ArgumentParser(description="A Catalogue Co-ordinator for SEDIMARK infrastructure.")
    # parser.add_argument('-i', '--interval', type=int, default=60, help="Interval in seconds between fetches (default: 60)")
    # args = parser.parse_args()

    redis_config = node_list_setup()
    # You can now pass redis_config to get_nodes.py functions as needed

    server_process = Process(target=setup_server)
    server_process.start()
    logger.info("API server process started.")

    # Optionally, you can add logic here to do other work in the main process
    # For demonstration, we'll just keep the main process alive
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down Coordinator...")
        server_process.terminate()
        server_process.join()

if __name__ == "__main__":
    main()