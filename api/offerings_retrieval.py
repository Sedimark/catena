import logging
import requests
import re
from flask import Flask, request, jsonify, Response, current_app
from utils.dlt_comm.get_nodes import get_node_list
from config import GC_URL, GC_PORT
from rdflib.plugins.sparql import prepareQuery
from rdflib.plugins.sparql.parser import parseQuery
from rdflib.plugins.sparql.algebra import translateQuery, translateUpdate
from rdflib.plugins.sparql.sparql import Query

app = Flask(__name__)
logger = logging.getLogger(__name__)


def _gc_sparql_endpoint() -> str:
    return f"{GC_URL}:{GC_PORT}/catalogue/sparql"


def _convert_to_federated_query(query: str, nodes: list) -> str:
    """
    Convert a simple SPARQL query to a federated query using SERVICE clauses.
    Uses rdflib's SPARQL parser for validation and proper query transformation.
    """
    if not nodes:
        return query
    
    try:
        # First, validate the query by parsing it
        try:
            parsed_query = prepareQuery(query)
            query_type = parsed_query.algebra.name
            logger.debug(f"Parsed query type: {query_type}")
        except Exception as e:
            logger.warning(f"Failed to parse SPARQL query: {e}")
            # If parsing fails, we can still try string-based federation
            # but log a warning
            pass
        
        # Use regex-based parsing for more robust WHERE clause extraction
        return _build_federated_query_with_regex(query, nodes)
        
    except Exception as e:
        logger.error(f"Error converting query to federated: {e}")
        return query


def _build_federated_query_with_regex(query: str, nodes: list) -> str:
    """
    Build federated query using regex for robust WHERE clause extraction.
    This handles complex SPARQL queries better than simple string manipulation.
    """
    # Normalize whitespace and line endings
    normalized_query = re.sub(r'\s+', ' ', query.strip())
    
    # Pattern to match WHERE clause with proper brace matching
    # This regex looks for WHERE followed by optional modifiers and then the pattern
    where_pattern = r'(SELECT\s+(?:DISTINCT\s+)?(?:REDUCED\s+)?[^{]+)\s+WHERE\s+(\{.*\})'
    
    match = re.search(where_pattern, normalized_query, re.IGNORECASE | re.DOTALL)
    
    if not match:
        # Try patterns for other query types
        patterns = [
            r'(ASK\s+)\s*(\{.*\})',  # ASK queries
            r'(CONSTRUCT\s+(?:\{[^}]*\}\s+)?WHERE\s+)(\{.*\})',  # CONSTRUCT queries
            r'(DESCRIBE\s+[^{]+\s+WHERE\s+)(\{.*\})',  # DESCRIBE queries
        ]
        
        for pattern in patterns:
            match = re.search(pattern, normalized_query, re.IGNORECASE | re.DOTALL)
            if match:
                break
    
    if not match:
        logger.warning("Could not find WHERE clause pattern in query")
        return query
    
    prefix_part = match.group(1).strip()
    where_content = match.group(2).strip()
    
    # Remove the outer braces from WHERE content
    if where_content.startswith('{') and where_content.endswith('}'):
        where_content = where_content[1:-1].strip()
    
    # Build federated query with SERVICE clauses
    federated_services = []
    for node in nodes:
        node_url = node.get('node_url', '')
        if node_url:
            federated_services.append(f'SERVICE <{node_url}> {{ {where_content} }}')
    
    if not federated_services:
        return query
    
    # Reconstruct the federated query
    if 'SELECT' in prefix_part.upper():
        # For SELECT queries, use UNION
        federated_query = f"{prefix_part} WHERE {{\n" + '\nUNION\n'.join(federated_services) + "\n}"
    else:
        # For other query types, we might need different federation strategies
        # For now, use UNION for all types
        federated_query = f"{prefix_part} {{\n" + '\nUNION\n'.join(federated_services) + "\n}"
    
    return federated_query


def _build_federated_update_query(original_query: str, nodes: list) -> str:
    """Build federated query for UPDATE operations (INSERT/DELETE)."""
    # UPDATE queries are complex to federate because they modify data
    # For now, return the original query as federated updates require
    # careful consideration of data consistency
    logger.warning("Federated UPDATE queries not yet implemented, returning original query")
    return original_query


@app.route("/sparql", methods=["POST"])
def forward_sparql_to_gc():
    try:
        content_type = request.headers.get("Content-Type", "")
        accept = request.headers.get("Accept", "")

        # Extract the original query
        original_query = None
        if request.is_json:
            body = request.get_json(silent=True) or {}
            original_query = body.get("query")
        elif "application/sparql-query" in content_type:
            original_query = request.get_data().decode("utf-8")
        elif "application/x-www-form-urlencoded" in content_type:
            # Handle form data
            form_data = request.form.to_dict()
            original_query = form_data.get("query")
        else:
            # Fallback: try to get raw data as query
            original_query = request.get_data().decode("utf-8")
        
        if not original_query:
            return jsonify({"error": "No query provided"}), 400

        # Get nodes from Redis to create federated query
        nodes = get_node_list(current_app.config["REDIS_CONFIG"])
        if not nodes:
            logger.warning("No nodes available for federation")
            # If no nodes, send original query as-is
            federated_query = original_query
        else:
            # Convert to federated query
            federated_query = _convert_to_federated_query(original_query, nodes)
            logger.info(f"Converted query to federated query with {len(nodes)} services")

        # Prepare forwarding to Global Catalogue
        forward_headers = {}
        if accept:
            forward_headers["Accept"] = accept
        forward_headers["Content-Type"] = "application/sparql-query"

        gc_url = _gc_sparql_endpoint()
        resp = requests.post(gc_url, data=federated_query.encode("utf-8"), headers=forward_headers, timeout=30)

        # Build Flask Response mirroring GC response
        response = Response(resp.content, status=resp.status_code)
        ct = resp.headers.get("Content-Type")
        if ct:
            response.headers["Content-Type"] = ct
        return response
    except requests.RequestException as e:
        logger.error(f"Failed to forward SPARQL to GC: {e}")
        return jsonify({"error": "Failed to reach Global Catalogue"}), 502
    except Exception as e:
        logger.exception("Unexpected error while forwarding SPARQL")
        return jsonify({"error": "Internal server error"}), 500
