# Environment Variables Configuration

This document lists all the environment variables that can be configured for the Catalogue Coordinator system.

## Flask API Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `HOST_ADDRESS` | `0.0.0.0` | The host address to bind the Flask API server to |
| `HOST_PORT` | `5000` | The port number for the Flask API server |

## DLT Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `DLT_BASE_URL` | `http://localhost:8080` | Base URL for the DLT Booth service |

## Redis Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `REDIS_HOST` | `localhost` | Redis server hostname or IP address |
| `REDIS_PORT` | `6379` | Redis server port number |
| `REDIS_DB` | `0` | Redis database number to use |

## Worker Pool Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `WORKER_POOL_SIZE` | `10` | Number of worker threads in the worker pool |

## Node Monitoring Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `NODE_HEALTH_CHECK_INTERVAL` | `30` | Interval in seconds between node health checks |
| `NODE_GRACE_PERIOD` | `60` | Grace period in seconds before marking a node as unhealthy |
| `NODE_TIMEOUT` | `10` | Timeout in seconds for individual node health check requests |

## Hash Ring Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `HASH_RING_VIRTUAL_NODES` | `150` | Number of virtual nodes per physical node for consistent hashing |

## Example .env File

To use these environment variables, create a `.env` file in your project root with the following content:

```bash
# Flask API Configuration
HOST_ADDRESS=0.0.0.0
HOST_PORT=5000

# DLT Configuration
DLT_BASE_URL=http://localhost:8080

# Redis Configuration
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_DB=0

# Worker Pool Configuration
WORKER_POOL_SIZE=10

# Node Monitoring Configuration
NODE_HEALTH_CHECK_INTERVAL=30
NODE_GRACE_PERIOD=60
NODE_TIMEOUT=10

# Hash Ring Configuration
HASH_RING_VIRTUAL_NODES=150
```

## Production Recommendations

For production environments, consider:

1. **Security**: Use non-default ports and restrict HOST_ADDRESS
2. **Performance**: Adjust WORKER_POOL_SIZE based on your server capacity
3. **Reliability**: Increase NODE_GRACE_PERIOD for more stable networks
4. **Scalability**: Adjust HASH_RING_VIRTUAL_NODES based on expected node count

## Docker Usage

When using Docker, you can pass these environment variables via:

```bash
docker run -e REDIS_HOST=redis-server -e DLT_BASE_URL=http://dlt-server:8080 ...
```

Or in docker-compose.yml:

```yaml
environment:
  - REDIS_HOST=redis
  - DLT_BASE_URL=http://dlt:8080
```
