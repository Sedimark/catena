# Loading environment variables (docker-compose env_file issue workaround)
set -a
source ../.env
set +a

# Docker compose
docker compose up -d
