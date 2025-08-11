"""
Redis client with connection management and fallback logic.
"""

import redis
from typing import Optional

from .fallback import MemoryKV


def get_redis_client(host: str, port: int, db: int, password: Optional[str] = None):
    """
    Get Redis client with fallback to in-memory storage.
    
    Args:
        host: Redis host
        port: Redis port
        db: Redis database number
        password: Redis password (optional)
    
    Returns:
        Redis client or MemoryKV fallback
    """
    try:
        client = redis.Redis(
            host=host,
            port=port,
            db=db,
            password=password,
            decode_responses=True,
            socket_connect_timeout=2,
            socket_timeout=5,
            retry_on_timeout=True
        )
        # Test connection
        client.ping()
        return client
    except Exception:
        return MemoryKV()
