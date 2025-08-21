from flask import Flask, request, jsonify
import os
from utils import OfferingProcessor

# Create the Flask app as a variable so it can be imported elsewhere
app = Flask(__name__)

def get_redis_config():
    """Get Redis configuration from environment variables."""
    return {
        'host': os.getenv('REDIS_HOST', 'catalogue-coordinator-redis'),
        'port': int(os.getenv('REDIS_PORT', 6379)),
        'db': int(os.getenv('REDIS_DB', 0)),
    }

def retrieve_offerings_by_id(offerings_id: str) -> dict:
    """
    Retrieve and process an offering by ID.
    """
    try:
        redis_config = get_redis_config()
        offering_processor = OfferingProcessor(redis_config)
        
        # Get the offering status from Redis
        status = offering_processor.get_offering_status(offerings_id)
        
        if status:
            return {
                "status": "success",
                "message": f"Offering {offerings_id} found",
                "offering_id": offerings_id,
                "assigned_node": status['assigned_node'],
                "offering_status": status['status']
            }
        else:
            return {
                "status": "error",
                "message": f"Offering {offerings_id} not found or not yet processed",
                "offering_id": offerings_id
            }
            
    except Exception as e:
        return {
            "status": "error",
            "message": f"Error retrieving offering {offerings_id}: {str(e)}",
            "offering_id": offerings_id
        }

@app.route('/offerings', methods=['POST'])
def get_offerings():
    """
    Process an offering by ID.
    """
    data = request.get_json()
    offerings_id = data.get('offerings_id')
    
    if not offerings_id:
        return jsonify({
            "status": "error",
            "message": "'offerings_id' is required."
        }), 400
    
    result = retrieve_offerings_by_id(offerings_id)
    return jsonify(result)

@app.route('/offerings/process', methods=['POST'])
def process_offerings():
    """
    Process all available offerings from DLT.
    """
    try:
        redis_config = get_redis_config()
        offering_processor = OfferingProcessor(redis_config)
        
        # Get all offerings from DLT
        from utils import get_offerings_for_processing
        offering_ids, offering_meta = get_offerings_for_processing(redis_config)
        
        if not offering_meta:
            return jsonify({
                "status": "error",
                "message": "No offerings found to process"
            }), 404
        
        # Process all offerings
        results = offering_processor.process_multiple_offerings(offering_meta)
        
        # Count successes and failures
        success_count = sum(1 for success in results.values() if success)
        failure_count = len(results) - success_count
        
        return jsonify({
            "status": "success",
            "message": f"Processed {len(results)} offerings",
            "results": {
                "total": len(results),
                "successful": success_count,
                "failed": failure_count,
                "details": results
            }
        })
        
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": f"Error processing offerings: {str(e)}"
        }), 500

@app.route('/offerings/status/<offering_id>', methods=['GET'])
def get_offering_status(offering_id: str):
    """
    Get the current status of a specific offering.
    """
    result = retrieve_offerings_by_id(offering_id)
    return jsonify(result)

# Note: Do NOT run app.run() here. This file is meant to be imported and run from another file.

