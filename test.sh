#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

compose_file="${COMPOSE_FILE:-compose.yml}"
minio_user="${MINIO_ROOT_USER:-gritadmin}"
minio_password="${MINIO_ROOT_PASSWORD:-gritadmin123}"
minio_bucket="${MINIO_BUCKET:-grit-server}"
nats_port="${GRIT_LIB_SERVER_NATS_PORT:-4222}"
minio_port="${GRIT_LIB_SERVER_MINIO_PORT:-9000}"

compose=()
container_engine=""

if command -v podman-compose >/dev/null 2>&1; then
  compose=(podman-compose -f "$compose_file")
  container_engine="podman"
elif command -v podman >/dev/null 2>&1 && podman compose version >/dev/null 2>&1; then
  compose=(podman compose -f "$compose_file")
  container_engine="podman"
elif command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1; then
  compose=(docker compose -f "$compose_file")
  container_engine="docker"
elif command -v docker-compose >/dev/null 2>&1; then
  compose=(docker-compose -f "$compose_file")
  container_engine="docker"
fi

tcp_open() {
  local host="$1"
  local port="$2"

  (echo >"/dev/tcp/$host/$port") >/dev/null 2>&1
}

wait_for_tcp() {
  local host="$1"
  local port="$2"
  local name="$3"
  local attempts=60

  for _ in $(seq 1 "$attempts"); do
    if tcp_open "$host" "$port"; then
      return 0
    fi
    sleep 1
  done

  echo "Timed out waiting for $name at $host:$port" >&2
  return 1
}

free_port() {
  python3 - <<'PY'
import socket

with socket.socket() as sock:
    sock.bind(("127.0.0.1", 0))
    print(sock.getsockname()[1])
PY
}

if [[ -n "${GRIT_LIB_SERVER_REDIS_PORT:-}" ]]; then
  redis_port="$GRIT_LIB_SERVER_REDIS_PORT"
elif tcp_open 127.0.0.1 6379; then
  redis_port="$(free_port)"
  echo "Port 6379 is already in use; starting test Redis on $redis_port."
else
  redis_port=6379
fi

export GRIT_LIB_SERVER_REDIS_PORT="$redis_port"
export GRIT_LIB_SERVER_NATS_PORT="$nats_port"
export GRIT_LIB_SERVER_MINIO_PORT="$minio_port"

if [[ "${GRIT_LIB_SERVER_SKIP_CONTAINERS:-0}" != "1" ]]; then
  if ! tcp_open 127.0.0.1 "$minio_port"; then
    if [[ ${#compose[@]} -eq 0 ]]; then
      echo "podman/podman-compose or docker/docker-compose is required to start test services." >&2
      exit 1
    fi

    echo "Starting test services with $container_engine: minio..."
    "${compose[@]}" rm -sf minio >/dev/null 2>&1 || true
    "${compose[@]}" up -d minio
  fi

  wait_for_tcp 127.0.0.1 "$minio_port" MinIO

  if [[ ${#compose[@]} -gt 0 ]]; then
    "${compose[@]}" run --rm minio-init
  fi

  services=()
  if ! tcp_open 127.0.0.1 "$redis_port"; then
    services+=(redis)
  fi
  if ! tcp_open 127.0.0.1 "$nats_port"; then
    services+=(nats)
  fi

  if [[ ${#services[@]} -gt 0 ]]; then
    if [[ ${#compose[@]} -eq 0 ]]; then
      echo "podman/podman-compose or docker/docker-compose is required to start test services." >&2
      exit 1
    fi

    echo "Starting test services with $container_engine: ${services[*]}..."
    "${compose[@]}" rm -sf "${services[@]}" >/dev/null 2>&1 || true
    "${compose[@]}" up -d "${services[@]}"
  else
    echo "Redis, NATS, and MinIO ports are already open; using existing services."
  fi

  wait_for_tcp 127.0.0.1 "$redis_port" Redis
  wait_for_tcp 127.0.0.1 "$nats_port" NATS
fi

export GRIT_LIB_SERVER_TEST_SCRIPT=1
export GRIT_LIB_SERVER_REDIS_URL="${GRIT_LIB_SERVER_REDIS_URL:-redis://127.0.0.1:$redis_port}"
export GRIT_LIB_SERVER_NATS_URL="${GRIT_LIB_SERVER_NATS_URL:-nats://127.0.0.1:$nats_port}"
export GRIT_LIB_SERVER_S3_BUCKET="${GRIT_LIB_SERVER_S3_BUCKET:-$minio_bucket}"
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-$minio_user}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-$minio_password}"
export AWS_REGION="${AWS_REGION:-us-east-1}"
export AWS_ENDPOINT_URL="${AWS_ENDPOINT_URL:-http://127.0.0.1:$minio_port}"

cargo test -p grit-lib-server \
  --features live-redis-tests,live-nats-tests,live-s3-tests \
  -- --ignored
