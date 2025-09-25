# Catena - Decentralised Catalogue Coordinator

Catena is a distributed catalogue coordination system that retrieves offerings from DLT-Booth, distributes data across catalogue nodes using consistent hashing, and provides federated SPARQL querying capabilities.

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
├── api
│   ├── __init__.py
│   └── offerings_retrieval.py
├── config.py
├── docker
│   ├── docker-compose.yml
│   ├── Dockerfile
│   └── start.sh
├── examples
│   ├── catalogue_list.json
│   └── example.env
├── LICENSE
├── main.py
├── README.md
├── requirements.txt
└── utils
    ├── __init__.py
    ├── dlt_comm
    │   ├── get_nodes.py
    │   ├── __init__.py
    │   └── offering_processor.py
    ├── hash_ring
    │   ├── consistent_hash.py
    │   └── __init__.py
    ├── node_monitor
    │   ├── health_checker.py
    │   └── __init__.py
    └── workers
        ├── data_processor.py
        ├── __init__.py
        └── worker_pool.py
```

### Data Flow

1. **Offerings Retrieval**: Coordinator fetches addresses from `DLT_BASE_URL/offerings` at configurable intervals
2. **Data Distribution**: Each offering is fetched and distributed to catalogue nodes using consistent hashing
3. **Redundancy**: Data is replicated across multiple nodes (configurable via `REDUNDANCY_REPLICAS`)
4. **Node Monitoring**: Continuous health checks detect node failures and trigger data redistribution
5. **Federated Queries**: SPARQL queries are distributed across all active catalogue nodes

## Configuration

Create an `.env` file (use `./env/example.env` as a reference):

## Installation & Usage

### Option 1: Docker Compose (Recommended)

1. **Start with Docker** (includes Redis):
   ```bash
   cd docker
   bash start.sh
   ```

2. **Stop services**:
   ```bash
   cd docker
   docker-compose down
   ```

3. **View logs**:
   ```bash
   cd docker
   docker-compose logs -f
   ```

### Option 2: Local Development

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
- **Consistent Hashing**: Uses consistent hashing for stable data distribution
- **Graceful Degradation**: Falls back to in-memory storage if Redis is unavailable
- **Configurable Redundancy**: Supports multiple data replicas for high availability
- **Docker Support**: Containerized deployment with Redis as separate service
- **Health Monitoring**: Built-in health checks for Docker orchestration

## Notes

The coordinator has two modes, set using the variable `BASELINE_INFRA`
- `BASEINE_INFRA: 0`: Enables the decentralised mode
- `BASELINE_INFRA: 1`: Defaults to centralised mode and refers to catalogue nodes from `./catalogue_list.json` (example file under `./examples` directory)

## TODO

- **Add `/profile` call step**: Add an additional call step to provider to fetch catalogue endpoints
- **Clarify federated SPARQL query**: Clarify if consumers directly call GC
- **Add tests**: Add test cases for all submodules
