#!/bin/bash

# Catalogue Coordinator Startup Script

echo "🚀 Starting Catalogue Coordinator..."

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker first."
    exit 1
fi

# Check if docker-compose is available
if ! command -v docker-compose &> /dev/null; then
    echo "❌ docker-compose is not available. Please install it first."
    exit 1
fi

# Check if virtual environment exists for local development
if [ ! -d ".venv" ]; then
    echo "📦 Creating virtual environment for local development..."
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

# Redis configuration (will be overridden by Docker Compose)
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_DB=0
REDIS_NODES_KEY=catalogue:nodes
REDIS_DATA_KEY_PREFIX=catalogue:data:
EOF
fi

echo "🐳 Starting services with Docker Compose..."
echo "   - Redis container will be started"
echo "   - Catalogue coordinator will be built and started"
echo "   - Services will be available on localhost:8000 and localhost:6379"

# Start services
docker-compose up --build -d

echo ""
echo "✅ Services started successfully!"
echo ""
echo "📊 Service Status:"
docker-compose ps
echo ""
echo "🔍 View logs: docker-compose logs -f"
echo "🛑 Stop services: docker-compose down"
echo "🌐 Access coordinator: http://localhost:8000"
echo "🗄️  Redis: localhost:6379"
echo ""
echo "🧪 Test the coordinator:"
echo "   curl http://localhost:8000/health"
