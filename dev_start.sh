#!/bin/bash

# Development Start Script (without Docker)

echo "🔧 Starting Catalogue Coordinator in development mode..."

# Check if virtual environment exists
if [ ! -d ".venv" ]; then
    echo "📦 Creating virtual environment..."
    python3 -m venv .venv
fi

# Activate virtual environment
echo "🔧 Activating virtual environment..."
source .venv/bin/activate

# Install/update dependencies
echo "📚 Installing dependencies..."
pip install -r requirements.txt

# Check if .env exists
if [ ! -f ".env" ]; then
    echo "⚠️  No .env file found. Creating default configuration..."
    cat > .env << 'EOF'
# Host configuration
HOST_ADDRESS=0.0.0.0
HOST_PORT=8000

# DLT Booth configuration
DLT_BASE_URL=http://localhost:5001
DLT_OFFERINGS_PATH=/offerings
DLT_NODES_PATH=/nodes

# Coordinator timing
FETCH_INTERVAL_SECONDS=60
NODE_CHECK_INTERVAL_SECONDS=30

# Redundancy and replication
REDUNDANCY_REPLICAS=2

# Redis configuration (development mode - will use in-memory fallback)
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_DB=0
REDIS_NODES_KEY=catalogue:nodes
REDIS_DATA_KEY_PREFIX=catalogue:data:
EOF
fi

echo "⚠️  Development mode: Redis will use in-memory fallback"
echo "🎯 Starting coordinator..."
python3 main.py
