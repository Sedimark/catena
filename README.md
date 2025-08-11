# Catalogue Coordinator

A distributed catalogue coordination system that retrieves offerings from DLT-Booth, distributes data across catalogue nodes using consistent hashing, and provides federated SPARQL querying capabilities.

## Architecture

The coordinator consists of several key components organized in a clean, modular structure:

### Core Components

- **Main Coordinator** (`main.py`): Orchestrates all background workers and the API server
- **API Layer** (`api/`): Flask-based REST API with SPARQL federation
- **DLT Communication** (`utils/dlt_comm/`): Handles communication with DLT-Booth API
- **Monitoring** (`utils/monitoring/`): Node health monitoring and failover handling
- **Workers** (`utils/workers/`): Background workers for offerings processing
- **Hashring** (`utils/hashring/`): Consistent hashing for data distribution
- **Redis** (`utils/redis/`): Redis client with graceful fallback to in-memory storage

### Directory Structure

```
utils/
├── dlt_comm/          # DLT-Booth communication
│   ├── __init__.py
│   └── get_nodes.py
├── hashring/          # Consistent hashing utilities
│   ├── __init__.py
│   └── ketama.py
├── monitoring/        # Node monitoring and health checks
│   ├── __init__.py
│   ├── health_checker.py
│   └── node_monitor.py
├── redis/            # Redis operations and fallback
│   ├── __init__.py
│   ├── client.py
│   └── fallback.py
└── workers/          # Background workers
    ├── __init__.py
    ├── data_processor.py
    └── offering_worker.py
```

### Data Flow

1. **Offerings Retrieval**: Coordinator fetches addresses from `DLT_BASE_URL/offerings` at configurable intervals
2. **Data Distribution**: Each offering is fetched and distributed to catalogue nodes using libketama consistent hashing
3. **Redundancy**: Data is replicated across multiple nodes (configurable via `REDUNDANCY_REPLICAS`)
4. **Node Monitoring**: Continuous health checks detect node failures and trigger data redistribution
5. **Federated Queries**: SPARQL queries are distributed across all active catalogue nodes

## Configuration

Create a `.env` file with the following variables:

```bash
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

# Redis configuration
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_DB=0
REDIS_NODES_KEY=catalogue:nodes
REDIS_DATA_KEY_PREFIX=catalogue:data:
```

## Installation & Usage

### Option 1: Docker Compose (Recommended)

1. **Start with Docker** (includes Redis):
   ```bash
   ./start.sh
   ```

2. **Stop services**:
   ```bash
   docker-compose down
   ```

3. **View logs**:
   ```bash
   docker-compose logs -f
   ```

### Option 2: Local Development

1. **Start without Docker** (uses in-memory fallback):
   ```bash
   ./dev_start.sh
   ```

2. **Manual setup**:
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   python3 main.py
   ```

## API Endpoints

### Health Check
- **GET** `/health` - Service health status

### Offerings
- **POST** `/offerings` - Retrieve offerings by ID (JSON body: `{"offerings_id": "..."}`)

### SPARQL Federation
- **POST** `/sparql` - Execute federated SPARQL queries across all catalogue nodes

## Features

- **Retry Logic**: Uses tenacity for robust retry mechanisms with exponential backoff
- **Caching**: Implements TTL-based caching for API responses
- **Failover**: Automatic data redistribution when nodes go down
- **Consistent Hashing**: Uses libketama algorithm for stable data distribution
- **Graceful Degradation**: Falls back to in-memory storage if Redis is unavailable
- **Configurable Redundancy**: Supports multiple data replicas for high availability
- **Docker Support**: Containerized deployment with Redis as separate service
- **Health Monitoring**: Built-in health checks for Docker orchestration

## Dependencies

- **Flask**: Web framework for API endpoints
- **Redis**: Data storage and caching
- **Tenacity**: Retry logic and resilience
- **RDFLib**: RDF/SPARQL processing
- **uhashring**: Consistent hashing implementation
- **Requests**: HTTP client for external API calls

## Development

The system is designed to be easily extensible:

- **Add new DLT endpoints** in `utils/dlt_comm/`
- **Implement custom health checks** in `utils/monitoring/`
- **Extend SPARQL federation** in `api/offerings_retrieval.py`
- **Add new data distribution strategies** in `utils/hashring/`
- **Create new worker types** in `utils/workers/`

## Docker Architecture

The system runs as two containers:

1. **Redis Container**: Dedicated Redis instance with persistence
2. **Coordinator Container**: Python application with all utilities

```yaml
# docker-compose.yml
services:
  redis:           # Redis 7 with persistence
  catalogue-coordinator:  # Main application
```

## Notes

- **Placeholder Nodes**: Uses placeholder nodes if DLT-Booth is unavailable
- **Health Endpoints**: Node health checks expect `/health` endpoints on catalogue nodes
- **SPARQL Updates**: Updates are sent to `/update` endpoints on catalogue nodes
- **Graceful Fallback**: Works without Redis using in-memory storage
- **Docker Health Checks**: Built-in health monitoring for container orchestration
