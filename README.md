# Catena - Decentralised Catalogue Coordinator

Catena is a distributed catalogue coordination system that retrieves offerings from DLT-Booth, distributes data across catalogue nodes using consistent hashing, and provides federated SPARQL querying capabilities.

![Catena Block Diagram](https://i.ibb.co/Q3kyxKj7/Sedi-Mark-Entities.png)

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

## Environment Variables

### Coordinator Configuration
| Variable | Description | Default / Example |
|----------|-------------|-------------------|
| `SUBPROCESS_HEALTH_CHECK_INTERVAL` | Interval (seconds) for checking subprocess health. | `5` |
| `WORKER_POOL_SIZE` | Number of worker threads/processes in the pool. | `10` |
| `BASELINE_INFRA` | Coordinator mode toggle (0 = disabled, 1 = enabled). | `0` |

### Flask API Configuration
| Variable | Description | Default / Example |
|----------|-------------|-------------------|
| `HOST_ADDRESS` | Address for the Flask API to bind to. | `0.0.0.0` |
| `HOST_PORT` | Port for the Flask API to listen on. | `5000` |

### DLT Configuration
| Variable | Description | Default / Example |
|----------|-------------|-------------------|
| `DLT_BASE_URL` | Base URL for DLT Booth API. | `http://dlt-booth:8085/api` |
| `DLT_RUST_LOG` | Log level for DLT (`debug`, `info`, `error`). | `debug` |
| `DLT_RUST_BACKTRACE` | Enable backtrace on errors (`0` = off, `1` = on). | `1` |
| `DLT_HOST_ADDRESS` | DLT Booth HTTP server bind address. | `0.0.0.0` |
| `DLT_HOST_PORT` | DLT Booth HTTP server port. | `8085` |
| `DLT_NODE_URL` | DLT node endpoint URL. | `https://example.com/node` |
| `DLT_FAUCET_API_ENDPOINT` | Faucet API endpoint. | `https://example.com/faucet/` |
| `DLT_RPC_PROVIDER` | RPC provider endpoint. | `https://example.com/rpc` |
| `DLT_CHAIN_ID` | Chain ID for the DLT network. | `1000` |
| `DLT_ISSUER_URL` | Issuer service endpoint. | `https://example.com/issuer` |

### Redis Configuration
| Variable | Description | Default / Example |
|----------|-------------|-------------------|
| `REDIS_HOST` | Host address of Redis instance. | `catalogue-coordinator-redis` |
| `REDIS_PORT` | Redis port. | `6379` |
| `REDIS_DB` | Redis database index. | `0` |

### Offering Configuration
| Variable | Description | Default / Example |
|----------|-------------|-------------------|
| `OFFERING_DESC_TIMEOUT` | Timeout (seconds) for fetching offering description. | `60` |
| `OFFERING_FETCH_INTERVAL` | Interval (seconds) between offering fetch cycles. | `60` |
| `OFFERING_REPLICA_COUNT` | Number of replicas per offering. | `2` |

### Node Monitoring Configuration
| Variable | Description | Default / Example |
|----------|-------------|-------------------|
| `NODE_HEALTH_CHECK_INTERVAL` | Interval (seconds) for node health checks. | `30` |
| `NODE_GRACE_PERIOD` | Grace period (seconds) before marking node unhealthy. | `60` |
| `NODE_TIMEOUT` | Timeout (seconds) for node response. | `10` |

### Hash Ring Configuration
| Variable | Description | Default / Example |
|----------|-------------|-------------------|
| `HASH_RING_VIRTUAL_NODES` | Number of virtual nodes in the consistent hash ring. | `150` |

### Key Storage Configuration
| Variable | Description | Default / Example |
|----------|-------------|-------------------|
| `DLT_KEY_STORAGE_STRONGHOLD_SNAPSHOT_PATH` | Path to key storage snapshot file. | `./key_storage.stronghold` |
| `DLT_KEY_STORAGE_STRONGHOLD_PASSWORD` | Password for encrypting the key storage snapshot. | `some_hopefully_secure_password` |
| `DLT_KEY_STORAGE_MNEMONIC` | Mnemonic used to generate the key storage. | `your mnemonic here` |

### Wallet Configuration
| Variable | Description | Default / Example |
|----------|-------------|-------------------|
| `DLT_WALLET_STRONGHOLD_SNAPSHOT_PATH` | Path to wallet storage snapshot file. | `./wallet.stronghold` |
| `DLT_WALLET_STRONGHOLD_PASSWORD` | Password for encrypting the wallet snapshot. | `some_hopefully_secure_password` |

### Database Configuration
| Variable | Description | Default / Example |
|----------|-------------|-------------------|
| `DLT_BOOTH_DB_USER` | Username for DLT Booth database connection. | `postgres` |
| `DLT_BOOTH_DB_PASSWORD` | Password for DLT Booth database connection. | `dlt_booth` |

## Configuration

Create an `.env` file (use `./env/example.env` as a reference):

Alternatively, you can create a custom `.env` file with the help of the [enviroment variable descriptions](#environment-variables)

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
- `BASEINE_INFRA: 0`: Enables the decentralised mode where catalogue nodes are inferred and retrieved from the DLT offerings
- `BASELINE_INFRA: 1`: Defaults to using known catalogues and refers to catalogue nodes from `./catalogue_list.json` (example file under `./examples` directory)

## TODO

- **Federated Query Support**: Add support for all subqueries, not just `SELECT`
- **Add `/profile` call step**: Add an additional call step to provider to fetch catalogue endpoints
- **Clarify federated SPARQL query**: Clarify if consumers directly call GC
- **Add tests**: Add test cases for all submodules
