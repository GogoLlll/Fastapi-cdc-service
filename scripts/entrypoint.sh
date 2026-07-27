#!/usr/bin/env bash
set -euo pipefail

echo "ждём postgres на ${POSTGRES_HOST:-postgres}:${POSTGRES_PORT:-5432} ..."
for attempt in $(seq 1 60); do
    if python -c "
import os, socket, sys
host = os.getenv('POSTGRES_HOST', 'postgres')
port = int(os.getenv('POSTGRES_PORT', '5432'))
sock = socket.socket()
sock.settimeout(1)
try:
    sock.connect((host, port))
except OSError:
    sys.exit(1)
finally:
    sock.close()
"; then
        echo "postgres поднялся (попытка ${attempt})"
        break
    fi
    sleep 1
done

echo "применяем миграции ..."
alembic upgrade head

echo "запускаем: $*"
exec "$@"
