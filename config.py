import os
import logging
import dotenv

def load_config():
    dotenv.load_dotenv()

logger = logging.getLogger(__name__)

def validate_config():
    """Validate configuration values and log warnings for invalid values."""
    warnings = []
    
    # Validate port numbers
    if HOST_PORT <= 0 or HOST_PORT > 65535:
        warnings.append(f"Invalid HOST_PORT: {HOST_PORT}. Must be between 1-65535")
    
    if REDIS_PORT <= 0 or REDIS_PORT > 65535:
        warnings.append(f"Invalid REDIS_PORT: {REDIS_PORT}. Must be between 1-65535")
    
    # Validate worker pool size
    if WORKER_POOL_SIZE <= 0:
        warnings.append(f"Invalid WORKER_POOL_SIZE: {WORKER_POOL_SIZE}. Must be positive")
    elif WORKER_POOL_SIZE > 100:
        warnings.append(f"Large WORKER_POOL_SIZE: {WORKER_POOL_SIZE}. Consider if this is appropriate")
    
    # Validate timeouts and intervals
    if NODE_HEALTH_CHECK_INTERVAL <= 0:
        warnings.append(f"Invalid NODE_HEALTH_CHECK_INTERVAL: {NODE_HEALTH_CHECK_INTERVAL}. Must be positive")
    
    if NODE_GRACE_PERIOD <= 0:
        warnings.append(f"Invalid NODE_GRACE_PERIOD: {NODE_GRACE_PERIOD}. Must be positive")
    
    if NODE_TIMEOUT <= 0:
        warnings.append(f"Invalid NODE_TIMEOUT: {NODE_TIMEOUT}. Must be positive")
    
    if HASH_RING_VIRTUAL_NODES <= 0:
        warnings.append(f"Invalid HASH_RING_VIRTUAL_NODES: {HASH_RING_VIRTUAL_NODES}. Must be positive")
    
    # Log warnings
    for warning in warnings:
        logger.warning(f"Configuration warning: {warning}")
    
    if warnings:
        logger.warning(f"Found {len(warnings)} configuration warnings")

# Flask API Configuration
HOST_ADDRESS = os.getenv('HOST_ADDRESS', '0.0.0.0')
HOST_PORT = int(os.getenv('HOST_PORT', 5000))

# DLT Configuration
DLT_BASE_URL = os.getenv('DLT_BASE_URL', 'http://dlt-booth:8085/api')

# Redis Configuration
REDIS_HOST = os.getenv('REDIS_HOST', 'catalogue-coordinator-redis')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
REDIS_DB = int(os.getenv('REDIS_DB', 0))

# Worker Pool Configuration
WORKER_POOL_SIZE = int(os.getenv('WORKER_POOL_SIZE', 10))

# Node Monitoring Configuration
NODE_HEALTH_CHECK_INTERVAL = int(os.getenv('NODE_HEALTH_CHECK_INTERVAL', 30))
NODE_GRACE_PERIOD = int(os.getenv('NODE_GRACE_PERIOD', 60))
NODE_TIMEOUT = int(os.getenv('NODE_TIMEOUT', 10))

# Hash Ring Configuration
HASH_RING_VIRTUAL_NODES = int(os.getenv('HASH_RING_VIRTUAL_NODES', 150))

# Validate configuration on import
validate_config()