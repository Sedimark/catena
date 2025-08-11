"""
Background worker for fetching and distributing offerings.
"""

import json
import threading
import time
import logging
import requests
from typing import Dict, Any, List

from utils.redis import get_redis_client
from utils.hashring import KetamaRouter
from utils.dlt_comm.get_nodes import get_node_list
from .data_processor import DataProcessor

from config import (
    DLT_BASE_URL,
    DLT_OFFERINGS_PATH,
    FETCH_INTERVAL_SECONDS,
    REDIS_HOST,
    REDIS_PORT,
    REDIS_DB,
    REDIS_DATA_KEY_PREFIX,
    REDUNDANCY_REPLICAS,
)

logger = logging.getLogger(__name__)


class OfferingWorker:
    """
    Background worker that fetches offerings and distributes them to catalogue nodes.
    
    Runs continuously, fetching new offerings at configurable intervals
    and distributing them using consistent hashing.
    """
    
    def __init__(self, interval_seconds: int = FETCH_INTERVAL_SECONDS):
        """
        Initialize offering worker.
        
        Args:
            interval_seconds: Seconds between offering fetches
        """
        self.interval_seconds = interval_seconds
        self.data_processor = DataProcessor()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)
    
    def start(self):
        """Start the worker thread."""
        logger.info(f"Starting OfferingWorker with {self.interval_seconds}s interval")
        self._thread.start()
    
    def stop(self):
        """Stop the worker thread."""
        logger.info("Stopping OfferingWorker")
        self._stop.set()
        self._thread.join()
    
    def _run(self):
        """Main worker loop."""
        while not self._stop.is_set():
            try:
                self._process_offerings()
            except Exception as e:
                logger.error(f"Error in offering worker: {e}")
            
            time.sleep(self.interval_seconds)
    
    def _process_offerings(self):
        """Main processing logic for offerings."""
        # Get current node list
        nodes = get_node_list({
            "host": REDIS_HOST,
            "port": REDIS_PORT,
            "db": REDIS_DB,
            "key": "catalogue:nodes"
        })
        
        if not nodes:
            logger.warning("No nodes available for offering distribution")
            return
        
        # Create router for data distribution
        try:
            router = KetamaRouter(nodes)
        except ValueError as e:
            logger.error(f"Failed to create router: {e}")
            return
        
        # Fetch addresses from DLT-Booth
        addresses = self._fetch_addresses()
        if not addresses:
            logger.info("No addresses found in offerings")
            return
        
        # Process each address
        processed_count = 0
        for address in addresses:
            try:
                self._process_single_offering(address, router)
                processed_count += 1
            except Exception as e:
                logger.error(f"Failed to process offering {address}: {e}")
        
        logger.info(f"Processed {processed_count}/{len(addresses)} offerings")
    
    def _fetch_addresses(self) -> List[str]:
        """
        Fetch addresses from DLT-Booth offerings endpoint.
        
        Returns:
            List of offering addresses
        """
        try:
            url = f"{DLT_BASE_URL}{DLT_OFFERINGS_PATH}"
            response = requests.get(url, timeout=15)
            response.raise_for_status()
            payload = response.json()
            return payload.get("addresses", [])
        except Exception as e:
            logger.error(f"Failed to fetch offerings: {e}")
            return []
    
    def _process_single_offering(self, address: str, router: KetamaRouter):
        """
        Process a single offering address.
        
        Args:
            address: URL of the offering
            router: Hash ring router for node selection
        """
        # Fetch listing data
        listing = self.data_processor.fetch_listing(address)
        
        # Validate listing
        if not self.data_processor.validate_listing(listing):
            logger.warning(f"Invalid listing format for {address}")
            return
        
        # Extract offering ID for consistent hashing
        try:
            offering_id = self.data_processor.extract_offering_id(listing)
        except ValueError as e:
            logger.warning(f"Could not extract offering ID from {address}: {e}")
            offering_id = address  # Fallback to address
        
        # Get target nodes using consistent hashing
        targets = router.get_n(offering_id, max(1, REDUNDANCY_REPLICAS))
        target_ids = [t['id'] for t in targets]
        
        # Convert to SPARQL and distribute
        sparql = self.data_processor.listing_to_sparql_insert(listing)
        
        # Store pointer in Redis for failover reassignment
        self._store_offering_pointer(offering_id, address, target_ids)
        
        # Post to target nodes
        success_count = 0
        for target in targets:
            if self.data_processor.post_sparql(target['address'], sparql):
                success_count += 1
                logger.debug(f"Posted offering {offering_id} to {target['id']}")
            else:
                logger.warning(f"Failed to post offering {offering_id} to {target['id']}")
        
        logger.info(f"Distributed offering {offering_id} to {success_count}/{len(targets)} nodes")
    
    def _store_offering_pointer(self, offering_id: str, address: str, target_ids: List[str]):
        """
        Store offering pointer in Redis for failover handling.
        
        Args:
            offering_id: Unique identifier for the offering
            address: Source address of the offering
            target_ids: List of target node IDs
        """
        client = get_redis_client(REDIS_HOST, REDIS_PORT, REDIS_DB)
        key = f"{REDIS_DATA_KEY_PREFIX}{offering_id}"
        
        try:
            client.set(key, json.dumps({
                "address": address,
                "targets": target_ids,
                "timestamp": time.time()
            }))
        except Exception as e:
            logger.error(f"Failed to store offering pointer for {offering_id}: {e}")


# Convenience functions for external use
def fetch_listing(address: str) -> Dict[str, Any]:
    """Fetch listing from address (for external use)."""
    processor = DataProcessor()
    return processor.fetch_listing(address)


def listing_to_sparql_insert(listing: Dict[str, Any]) -> str:
    """Convert listing to SPARQL (for external use)."""
    processor = DataProcessor()
    return processor.listing_to_sparql_insert(listing)


def post_sparql(endpoint: str, query: str) -> bool:
    """Post SPARQL to endpoint (for external use)."""
    processor = DataProcessor()
    return processor.post_sparql(endpoint, query)
