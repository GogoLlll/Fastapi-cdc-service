set -uo pipefail

if [[ ! -f alembic.ini ]]; then
    echo "[x] Run this from the project root: ./scripts/check.sh"
    exit 1
fi

echo
echo "=== 1/6  Checking test dependencies ==="
if ! python -c "import httpx, websockets, uvicorn" 2>/dev/null; then
    cat <<'MSG'
[x] Test dependencies are missing. Install them first:

    python -m venv .venv
    source .venv/bin/activate
    pip install -r requirements-dev.txt

MSG
    exit 1
fi
echo "    ok"

echo
echo "=== 2/6  Stopping the app container ==="
docker compose stop app >/dev/null 2>&1 || true
echo "    ok"

echo
echo "=== 3/6  Starting PostgreSQL ==="
if ! docker compose up -d postgres; then
    echo "[x] Could not start PostgreSQL. Is Docker running?"
    exit 1
fi

for attempt in $(seq 1 30); do
    if docker compose exec -T postgres pg_isready -U outbox -d outbox >/dev/null 2>&1; then
        echo "    ready after ${attempt}s"
        break
    fi
    if [[ $attempt -eq 30 ]]; then
        echo "[x] PostgreSQL did not become ready in 30 seconds."
        exit 1
    fi
    sleep 1
done

export POSTGRES_HOST=localhost
export POSTGRES_PORT=5432
export POSTGRES_USER=outbox
export POSTGRES_PASSWORD=outbox
export POSTGRES_DB=outbox
export PYTHONPATH="$PWD"

echo
echo "=== 4/6  Applying migrations ==="
if ! alembic upgrade head; then
    echo "[x] Migrations failed."
    exit 1
fi

echo
echo "=== 5/6  Running the pytest suite ==="
python -m pytest
pytests=$?

echo
echo "=== 6/6  Running smoke tests ==="
echo
echo "--- stage 1: writes and outbox atomicity ---"
python scripts/smoke_stage1.py
stage1=$?

echo
echo "--- stage 2: event stream over a real WebSocket ---"
python scripts/smoke_stage2.py
stage2=$?

echo
echo "--- stage 3: two workers, retention, shutdown ---"
python scripts/smoke_stage3.py
stage3=$?

echo
echo "======================================================"
[[ $pytests -eq 0 ]] && echo "  pytest : OK" || echo "  pytest : FAILED"
[[ $stage1 -eq 0 ]] && echo "  stage 1: OK" || echo "  stage 1: FAILED"
[[ $stage2 -eq 0 ]] && echo "  stage 2: OK" || echo "  stage 2: FAILED"
[[ $stage3 -eq 0 ]] && echo "  stage 3: OK" || echo "  stage 3: FAILED"
echo "======================================================"
echo

if [[ $pytests -ne 0 || $stage1 -ne 0 || $stage2 -ne 0 || $stage3 -ne 0 ]]; then
    exit 1
fi
echo "All checks passed."
