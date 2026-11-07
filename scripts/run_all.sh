#!/usr/bin/env bash
# Opens the demo environment in tmux (docs/spec.md §8, §10):
#
#   +----------+----------+----------+
#   |  broker  |   S1 Py  |   S2 Py  |
#   | :8080    | sports   | news     |
#   +----------+----------+----------+
#   |   S3 C++ |publisher |          |
#   | sports   | (shell)  |          |
#   +----------+----------+----------+
#
# Every process runs in its own pane. Three things are recorded per pane:
#   @role        pane name (broker, S1, S2, S3, publisher)
#   @start_cmd   exact start command, so demo.sh can restart a killed subscriber
#   @log_file    file that receives everything the pane prints (tmux pipe-pane),
#                so demo.sh reads complete, byte-exact logs instead of screen contents
#
# Each run starts a fresh session and fresh log files. A session that already ran
# demo.sh cannot be reused: step 11 deregisters S3 while its process keeps running.
#
# Usage: scripts/run_all.sh
# Env:   PUBSUB_SESSION  tmux session name (default: pubsub)
#        SKIP_BUILD=1    skip the cmake configure/build step
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SESSION="${PUBSUB_SESSION:-pubsub}"
LOG_DIR="${TMPDIR:-/tmp}/${SESSION}-logs"
BROKER_PORT=8080
BROKER_URL="http://localhost:${BROKER_PORT}"
PORTS=(8080 9001 9002 9003)
PANES=()

die() {
  printf 'run_all.sh: %s\n' "$*" >&2
  exit 1
}

port_open() { # port -> success if something is listening on 127.0.0.1
  (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null
}

wait_port_free() { # port -> success once nothing listens on it (5 s max)
  local i
  for i in $(seq 1 25); do
    port_open "$1" || return 0
    sleep 0.2
  done
  return 1
}

spawn_pane() { # role start_command (empty for a plain shell)
  local role=$1 cmd=$2 pid log
  if ((${#PANES[@]} == 0)); then
    pid=$(tmux new-session -d -P -F '#{pane_id}' -s "$SESSION" -n demo -c "$ROOT" -x 200 -y 60)
  else
    pid=$(tmux split-window -P -F '#{pane_id}' -t "${PANES[-1]}" -c "$ROOT")
    tmux select-layout -t "${PANES[0]}" tiled
  fi
  PANES+=("$pid")

  # Start logging before the process starts so no output is missed.
  log="$LOG_DIR/$role.log"
  : >"$log"
  tmux pipe-pane -t "$pid" "cat >>$(printf '%q' "$log")"

  tmux select-pane -t "$pid" -T "$role"
  tmux set-option -p -t "$pid" @role "$role"
  tmux set-option -p -t "$pid" @log_file "$log"
  if [[ -n $cmd ]]; then
    tmux set-option -p -t "$pid" @start_cmd "$cmd"
    tmux send-keys -t "$pid" -l "$cmd"
    tmux send-keys -t "$pid" C-m
  fi
}

# ---- prerequisites and build -------------------------------------------------

for tool in tmux curl python3; do
  command -v "$tool" >/dev/null 2>&1 ||
    die "'$tool' not found. Install prerequisites: sudo apt install g++ cmake python3 tmux curl"
done

if [[ "${SKIP_BUILD:-0}" != 1 ]]; then
  for tool in cmake g++; do
    command -v "$tool" >/dev/null 2>&1 || die "'$tool' not found. Install: sudo apt install g++ cmake"
  done
  cmake -S "$ROOT" -B "$ROOT/build"
  cmake --build "$ROOT/build" -j
fi

BROKER_BIN="$ROOT/build/broker"
SUB_BIN="$ROOT/build/subscriber"
PY_SUB="$ROOT/subscribers/subscriber.py"
[[ -x $BROKER_BIN ]] || die "missing executable $BROKER_BIN (build first or unset SKIP_BUILD)"
[[ -x $SUB_BIN ]] || die "missing executable $SUB_BIN (build first or unset SKIP_BUILD)"
[[ -f $PY_SUB ]] || die "missing $PY_SUB"

printf -v BROKER_CMD '%q --port %s' "$BROKER_BIN" "$BROKER_PORT"
printf -v S1_CMD 'python3 %q --id S1 --port 9001 --topics sports --broker %s' "$PY_SUB" "$BROKER_URL"
printf -v S2_CMD 'python3 %q --id S2 --port 9002 --topics news --broker %s' "$PY_SUB" "$BROKER_URL"
printf -v S3_CMD '%q --id S3 --port 9003 --topics sports --broker %s' "$SUB_BIN" "$BROKER_URL"

# ---- tmux session --------------------------------------------------------------

if tmux has-session -t "$SESSION" 2>/dev/null; then
  echo "run_all.sh: replacing existing tmux session '$SESSION' (fresh logs and registrations)"
  tmux kill-session -t "$SESSION"
fi

for port in "${PORTS[@]}"; do
  wait_port_free "$port" || die "port $port is still in use by another process; free it and retry"
done

mkdir -p "$LOG_DIR"
rm -f -- "$LOG_DIR"/*.log

spawn_pane broker "$BROKER_CMD"
tmux set-option -w -t "$SESSION" pane-border-status top
tmux set-option -w -t "$SESSION" pane-border-format ' #{pane_title} '
tmux set-option -t "$SESSION" mouse on

for _ in $(seq 1 50); do
  curl -sf "$BROKER_URL/health" >/dev/null 2>&1 && break
  sleep 0.2
done
curl -sf "$BROKER_URL/health" >/dev/null 2>&1 ||
  die "broker did not answer on $BROKER_URL/health; inspect with: tmux attach -t $SESSION"

spawn_pane S1 "$S1_CMD"
spawn_pane S2 "$S2_CMD"
spawn_pane S3 "$S3_CMD"
spawn_pane publisher "echo 'Publisher pane: run ./scripts/demo.sh here and press Enter between steps.'"

tmux select-pane -t "${PANES[-1]}"

echo "Session '$SESSION' is running: broker, S1 (Py, sports), S2 (Py, news), S3 (C++, sports), publisher."
echo "Pane logs: $LOG_DIR/<role>.log"
echo "Run ./scripts/demo.sh in the publisher pane. Stop everything with: tmux kill-session -t $SESSION"

if [[ -n ${TMUX:-} ]]; then
  tmux switch-client -t "$SESSION"
elif [[ -t 1 ]]; then
  tmux attach-session -t "$SESSION"
else
  echo "No terminal attached; attach with: tmux attach -t $SESSION"
fi
