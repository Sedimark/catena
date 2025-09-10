"""
Data processing utilities for catalogue operations.
"""

import json
import logging
import requests
from typing import Dict, Any
from rdflib import Graph
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)


class DataProcessor:
    """Handles data processing operations for catalogue listings."""
    
    def __init__(self, timeout: int = 20):
        """
        Initialize data processor.
        
        Args:
            timeout: Request timeout in seconds
        """
        self.timeout = timeout
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=8))
    def fetch_listing(self, address: str) -> Dict[str, Any]:
        """
        Fetch listing data from a given address.
        
        Args:
            address: URL to fetch listing from
            
        Returns:
            Listing data as dictionary
            
        Raises:
            requests.RequestException: If fetch fails after retries
        """
        response = requests.get(address, timeout=self.timeout)
        response.raise_for_status()
        return response.json()
    
    def listing_to_sparql_insert(self, listing: Dict[str, Any]) -> str:
        """
        Convert JSON-LD listing to SPARQL INSERT statement.
        
        Args:
            listing: JSON-LD listing data
            
        Returns:
            SPARQL INSERT statement as string
        """
        try:
            g = Graph()
            g.parse(data=json.dumps(listing), format='json-ld')
            triples = g.serialize(format='nt')
            return f"INSERT DATA {{\n{triples}\n}}"
        except Exception as e:
            logger.error(f"Failed to convert listing to SPARQL: {e}")
            raise
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=8))
    def post_sparql(self, endpoint: str, query: str) -> bool:
        """
        Post SPARQL update to a catalogue node endpoint.
        
        Args:
            endpoint: Node endpoint URL
            query: SPARQL update query
            
        Returns:
            True if successful, False otherwise
        """
        headers = {"Content-Type": "application/sparql-update"}
        try:
            response = requests.post(
                f"{endpoint}/update",
                headers=headers,
                data=query,
                timeout=self.timeout
            )
            success = response.status_code in (200, 204)
            if not success:
                logger.warning(f"SPARQL update failed for {endpoint}: {response.status_code}")
            return success
        except Exception as e:
            logger.error(f"SPARQL update error for {endpoint}: {e}")
            return False
    
    def validate_listing(self, listing: Dict[str, Any]) -> bool:
        """
        Validate listing data structure.
        
        Args:
            listing: Listing data to validate
            
        Returns:
            True if valid, False otherwise
        """
        required_fields = ['@id', '@type']
        
        if not isinstance(listing, dict):
            return False
        
        for field in required_fields:
            if field not in listing:
                return False
        
        # Check if it's an offering (not a contract)
        offering_types = listing.get('@type', [])
        if isinstance(offering_types, str):
            offering_types = [offering_types]
        
        has_offering = any('Offering' in t for t in offering_types)
        has_contract = any('OfferingContract' in t for t in offering_types)
        
        return has_offering and not has_contract
    
    def extract_offering_id(self, listing: Dict[str, Any]) -> str:
        """
        Extract offering ID from listing.
        
        Args:
            listing: Listing data
            
        Returns:
            Offering ID string
            
        Raises:
            ValueError: If no valid offering ID found
        """
        offering_obj = [
            obj for obj in listing.get('@graph', [listing])
            if isinstance(obj, dict) and 
            ':Offering' in obj.get('@type', []) and 
            'OfferingContract' not in obj.get('@type', [])
        ]
        
        if len(offering_obj) != 1:
            raise ValueError("Error parsing self listing: no unique offering found")
        
        offering_id = offering_obj[0].get('@id')
        if not offering_id:
            raise ValueError("Error parsing self listing: no offering ID found")
        
        return offering_id
