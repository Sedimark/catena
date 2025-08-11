import os
from dotenv import load_dotenv

# Load .env
load_dotenv()

# variables for host server
HOST_ADDRESS = os.getenv("HOST_ADDRESS", "0.0.0.0")
HOST_PORT = int(os.getenv("HOST_PORT", "8000"))

# DLT Booth
DLT_BASE_URL = os.getenv("DLT_BASE_URL", "http://localhost:5001")
DLT_OFFERINGS_PATH = os.getenv("DLT_OFFERINGS_PATH", "/offerings")
DLT_NODES_PATH = os.getenv("DLT_NODES_PATH", "/nodes")

# Coordinator
FETCH_INTERVAL_SECONDS = int(os.getenv("FETCH_INTERVAL_SECONDS", "60"))
NODE_CHECK_INTERVAL_SECONDS = int(os.getenv("NODE_CHECK_INTERVAL_SECONDS", "30"))
REDUNDANCY_REPLICAS = int(os.getenv("REDUNDANCY_REPLICAS", "2"))

# Redis placeholder config
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))
REDIS_NODES_KEY = os.getenv("REDIS_NODES_KEY", "catalogue:nodes")
REDIS_DATA_KEY_PREFIX = os.getenv("REDIS_DATA_KEY_PREFIX", "catalogue:data:")