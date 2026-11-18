#!/usr/bin/env bash
# Start/stop the exam stack on the Ubuntu VM.
#   ./run.sh start   -> Kafka (Docker), topic, backend (port 8000), worker
#   ./run.sh stop    -> stop backend and worker (Kafka keeps running)
#   ./run.sh status  -> show processes and recent logs
set -euo pipefail
cd "$(dirname "$0")"
LOGS=data/logs
mkdir -p "$LOGS"

stop_apps() {
  for f in "$LOGS/api.pid" "$LOGS/worker.pid"; do
    if [[ -f $f ]] && kill "$(cat "$f")" 2>/dev/null; then echo "stopped $(basename "$f" .pid)"; fi
    rm -f "$f"
  done
}

case "${1:-start}" in
  start)
    stop_apps
    docker compose up -d --wait
    docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 \
      --create --if-not-exists --topic network-config-jobs --partitions 1 --replication-factor 1
    setsid nohup .venv/bin/uvicorn app.main:app --host 0.0.0.0 --port 8000 >"$LOGS/api.log" 2>&1 &
    echo $! >"$LOGS/api.pid"
    sleep 3
    setsid nohup .venv/bin/python -u worker.py >"$LOGS/worker.log" 2>&1 &
    echo $! >"$LOGS/worker.pid"
    sleep 2
    curl -fsS localhost:8000/health && echo
    tail -n 3 "$LOGS/worker.log"
    ;;
  stop) stop_apps ;;
  status)
    ps -o pid,args -p "$(cat $LOGS/api.pid 2>/dev/null || echo 1)" -p "$(cat $LOGS/worker.pid 2>/dev/null || echo 1)" || true
    echo "--- worker.log"; tail -n 15 "$LOGS/worker.log" 2>/dev/null || true
    echo "--- api.log"; tail -n 5 "$LOGS/api.log" 2>/dev/null || true
    ;;
  *) echo "usage: $0 start|stop|status"; exit 1 ;;
esac
