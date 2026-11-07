#!/usr/bin/env bash
# Scripted end-to-end demo for docs/spec.md §10.
#
# Run it in the publisher pane created by scripts/run_all.sh, or from any shell
# while that tmux session is alive. It pauses for Enter between the 11 steps.
#
# Observation: logs are read from the files that run_all.sh records in each pane's
# @log_file option (tmux pipe-pane). Reads are byte offsets taken at the start of
# each step, so only output produced by that step is inspected.
#
# Checks: each publish is checked against the delivery report (recipient set,
# status, attempts, ack), against which subscribers logged the event, and against
# the subscriber states reported by GET /subscribers. A mismatch stops the demo.
#
# Subscribers are driven through tmux: Ctrl+C in the S2 pane simulates a crash,
# and the S2 pane's stored @start_cmd restarts it with the same arguments.
#
# A session can run this demo once. Step 11 deregisters S3 while its process keeps
# running, so a second run in the same session fails at step 1. Start a fresh
# session with scripts/run_all.sh (it replaces the old one).
#
# Usage: scripts/demo.sh
# Env:   PUBSUB_SESSION  tmux session name (default: pubsub)
set -euo pipefail

BROKER="http://localhost:8080"
SESSION="${PUBSUB_SESSION:-pubsub}"
EVENT_ID=""
LAST_RESPONSE=""
declare -A MARK=()

die() {
  printf 'demo.sh: %s\n' "$*" >&2
  exit 1
}

# ---- tmux and log helpers ----------------------------------------------------

pane_for() { # role -> tmux pane id (role is stored in the @role pane option)
  local id role
  while read -r id; do
    role=$(tmux show-options -p -q -v -t "$id" @role)
    if [[ $role == "$1" ]]; then
      printf '%s\n' "$id"
      return 0
    fi
  done < <(tmux list-panes -s -t "$SESSION" -F '#{pane_id}')
  die "no '$1' pane in tmux session '$SESSION'; start the environment with scripts/run_all.sh"
}

log_file() { # role -> log file that pipe-pane writes for that pane
  local file
  file=$(tmux show-options -p -q -v -t "$(pane_for "$1")" @log_file)
  [[ -n $file && -f $file ]] || die "no log file for '$1'; start the environment with scripts/run_all.sh"
  printf '%s\n' "$file"
}

snapshot() { # remember the current size of each log in bytes
  local role
  for role in broker S1 S2 S3; do
    MARK[$role]=$(wc -c <"$(log_file "$role")")
  done
}

new_lines() { # role -> log text appended since the last snapshot, CRs removed
  tail -c "+$((${MARK[$1]:-0} + 1))" "$(log_file "$1")" | tr -d '\r'
}

expect_log() { # role pattern -> that role logged a line matching pattern (case-insensitive)
  new_lines "$1" | grep -i -F -- "$2" >/dev/null || die "$1 has no new log line containing '$2'"
}

# ---- broker helpers ----------------------------------------------------------

report() { # reads a /publish response on stdin and prints a delivery table
  python3 -c '
import json, sys
r = json.load(sys.stdin)
if "error" in r:
    print("  error: " + r["error"])
    sys.exit(1)
print("  event_id=%s mode=%s target=%s" % (r["event_id"], r["mode"], r.get("target", "-")))
if not r["deliveries"]:
    print("  (no recipients)")
for d in r["deliveries"]:
    extra = d.get("ack") or d.get("error", "")
    print("  %-4s %-12s attempts=%s  %s" % (d["id"], d["status"], d.get("attempts", "-"), extra))
'
}

publish() { # JSON body -> prints the delivery report; sets EVENT_ID and LAST_RESPONSE
  LAST_RESPONSE=$(curl -sS -X POST "$BROKER/publish" -H 'Content-Type: application/json' -d "$1") ||
    die "POST /publish failed; is the broker running?"
  printf '%s\n' "$LAST_RESPONSE" | report || die "broker rejected the publish: $LAST_RESPONSE"
  EVENT_ID=$(printf '%s' "$LAST_RESPONSE" | python3 -c 'import json, sys; print(json.load(sys.stdin)["event_id"])')
  sleep 0.5 # let subscriber and broker panes flush their log lines
}

expect_report() { # "ID:STATUS:ATTEMPTS:ACK ..." ('-' means not checked; empty means no deliveries)
  printf '%s' "$LAST_RESPONSE" | python3 -c '
import json, sys
want = {}
for item in sys.argv[1].split():
    parts = (item.split(":") + ["-", "-", "-"])[:4]
    want[parts[0]] = parts[1:]
got = {d["id"]: d for d in json.load(sys.stdin)["deliveries"]}
errors = []
if sorted(got) != sorted(want):
    errors.append("recipients are %s, expected %s" % (sorted(got), sorted(want)))
for sid in sorted(set(got) & set(want)):
    status, attempts, ack = want[sid]
    d = got[sid]
    if d["status"] != status:
        errors.append("%s status is %s, expected %s" % (sid, d["status"], status))
    if attempts != "-" and str(d.get("attempts")) != attempts:
        errors.append("%s attempts is %s, expected %s" % (sid, d.get("attempts"), attempts))
    if ack != "-" and d.get("ack") != ack:
        errors.append("%s ack is %s, expected %s" % (sid, d.get("ack"), ack))
if errors:
    print("  MISMATCH: " + "; ".join(errors))
    sys.exit(1)
print("  ok: delivery report matches [" + sys.argv[1] + "]")
' "$1" || die "delivery report does not match: $1"
}

expect_receivers() { # "S1 S3" event_id -> exactly these subscribers logged the event
  local expected=$1 ev=$2 role count
  for role in S1 S2 S3; do
    count=$(new_lines "$role" | grep -c -F -- "$ev" || true)
    if [[ " $expected " == *" $role "* ]]; then
      ((count > 0)) || die "$role did not log $ev"
    else
      ((count == 0)) || die "$role logged $ev but was not a recipient"
    fi
  done
  printf '  ok: subscribers that logged %s are [%s]\n' "$ev" "$expected"
}

sub_state() { # id -> ALIVE | DEAD | missing
  curl -sS "$BROKER/subscribers" | python3 -c '
import json, sys
want = sys.argv[1]
for s in json.load(sys.stdin):
    if s["id"] == want:
        print(s["state"])
        break
else:
    print("missing")
' "$1"
}

wait_state() { # id state timeout_s -> success once the broker reports that state
  local end=$((SECONDS + $3))
  while ((SECONDS < end)); do
    [[ $(sub_state "$1") == "$2" ]] && return 0
    sleep 0.5
  done
  return 1
}

expect_sub() { # id state failures -> broker reports that state and failure count
  curl -sS "$BROKER/subscribers" | python3 -c '
import json, sys
sid, state, failures = sys.argv[1], sys.argv[2], int(sys.argv[3])
subs = {s["id"]: s for s in json.load(sys.stdin)}
s = subs.get(sid)
if s is None:
    print("  " + sid + " is not registered")
    sys.exit(1)
if s["state"] != state or s["failures"] != failures:
    print("  %s is %s with failures=%d (expected %s, failures=%d)" % (sid, s["state"], s["failures"], state, failures))
    sys.exit(1)
print("  ok: %s is %s with failures=%d" % (sid, state, failures))
' "$1" "$2" "$3" || die "subscriber $1 does not match: state $2, failures $3"
}

expect_state() { # id state
  local got
  got=$(sub_state "$1")
  [[ $got == "$2" ]] || die "expected $1 to be $2 but broker reports '$got'"
}

show_subscribers() {
  curl -sS "$BROKER/subscribers" | python3 -c '
import json, sys
print("  %-4s %-8s %-9s %-10s %s" % ("id", "state", "failures", "delivered", "topics"))
for s in json.load(sys.stdin):
    print("  %-4s %-8s %-9s %-10s %s" % (s["id"], s["state"], s["failures"], s["delivered"], ",".join(s["topics"])))
'
}

show_broker_new() {
  printf '    broker log:\n'
  new_lines broker | sed 's/^/      /' || true
}

show_logs() { # event id -> what the broker and each subscriber logged since the snapshot
  local ev=$1 role lines
  show_broker_new
  for role in S1 S2 S3; do
    lines=$(new_lines "$role" | grep -F -- "$ev" || true)
    if [[ -n $lines ]]; then
      printf '    %s logged:\n' "$role"
      sed 's/^/      /' <<<"$lines"
    else
      printf '    %s: nothing for %s\n' "$role" "$ev"
    fi
  done
}

wait_port_closed() { # port timeout_s -> success once nothing accepts connections there
  local end=$((SECONDS + $2))
  while ((SECONDS < end)); do
    curl -s -o /dev/null --max-time 1 "http://localhost:$1/" || return 0
    sleep 0.5
  done
  return 1
}

# ---- presentation ------------------------------------------------------------

pause() {
  read -r -p "    >> press Enter to continue " _ || true
  echo
}

step() { # number title expected-observation
  if (($1 > 1)); then
    pause
  fi
  printf '\n==== Step %s/11: %s\n     expected: %s\n\n' "$1" "$2" "$3"
  snapshot
}

# ---- preflight ---------------------------------------------------------------

tmux has-session -t "$SESSION" 2>/dev/null ||
  die "no tmux session '$SESSION'; start the environment with scripts/run_all.sh"
for role in broker S1 S2 S3 publisher; do
  pane_for "$role" >/dev/null
  log_file "$role" >/dev/null
done
curl -sf "$BROKER/health" >/dev/null || die "broker is not healthy at $BROKER"

printf 'Demo of docs/spec.md §10 against the tmux session "%s".\n' "$SESSION"
printf 'Panes: broker | S1 (Python, sports) | S2 (Python, news) | S3 (C++, sports) | publisher.\n'
pause

# ---- §10 steps ---------------------------------------------------------------

step 1 "Start broker, S1 (Python, sports), S2 (Python, news), S3 (C++, sports)" \
  "three REGISTER lines in the broker log; all three subscribers ALIVE"
for s in S1 S2 S3; do
  wait_state "$s" ALIVE 20 ||
    die "$s is not ALIVE after 20 s. If demo.sh already ran in this session, step 11 deregistered S3; start a fresh session with ./scripts/run_all.sh"
done
show_subscribers
printf '    broker log:\n'
grep -F REGISTER "$(log_file broker)" | tr -d '\r' | sed 's/^/      /' || true
registers=$(grep -c -F REGISTER "$(log_file broker)" || true)
((registers == 3)) || die "expected 3 REGISTER lines in the broker log, found $registers; start a fresh session with ./scripts/run_all.sh"
printf '  ok: 3 REGISTER lines, all subscribers ALIVE\n'

step 2 "Unicast to S2" \
  "only S2 logs the event; report has one DELIVERED"
publish '{"mode": "unicast", "target": "S2", "payload": {"msg": "hello, S2 only"}}'
expect_report "S2:DELIVERED:1:processed"
show_logs "$EVENT_ID"
expect_receivers "S2" "$EVENT_ID"

step 3 "Multicast to topic sports (explicit event_id score-2-1)" \
  "S1 and S3 log the event; S2 silent"
publish '{"mode": "multicast", "target": "sports", "event_id": "score-2-1", "payload": {"score": "2-1"}}'
expect_report "S1:DELIVERED:1:processed S3:DELIVERED:1:processed"
show_logs "$EVENT_ID"
expect_receivers "S1 S3" "$EVENT_ID"

step 4 "Broadcast (explicit event_id notice-1)" \
  "S1, S2 and S3 all log the event"
publish '{"mode": "broadcast", "event_id": "notice-1", "payload": {"notice": "maintenance at 18:00"}}'
expect_report "S1:DELIVERED:1:processed S2:DELIVERED:1:processed S3:DELIVERED:1:processed"
show_logs "$EVENT_ID"
expect_receivers "S1 S2 S3" "$EVENT_ID"

step 5 "Publish again with the already-used event_id notice-1" \
  "every subscriber logs DUPLICATE ... ignored; report shows ack duplicate"
publish '{"mode": "broadcast", "event_id": "notice-1", "payload": {"notice": "maintenance at 18:00"}}'
expect_report "S1:DELIVERED:1:duplicate S2:DELIVERED:1:duplicate S3:DELIVERED:1:duplicate"
show_logs "$EVENT_ID"
expect_receivers "S1 S2 S3" "$EVENT_ID"
for role in S1 S2 S3; do
  expect_log "$role" duplicate
done

step 6 "Kill S2 (Ctrl+C in its pane), then broadcast" \
  "broker logs failed attempts and MARK DEAD S2; S1, S3 DELIVERED; S2 FAILED after 3 attempts"
tmux send-keys -t "$(pane_for S2)" C-c
wait_port_closed 9002 10 || die "port 9002 still accepts connections 10 s after Ctrl+C; S2 did not stop"
publish '{"mode": "broadcast", "payload": {"notice": "S2 is down"}}'
expect_report "S1:DELIVERED:1:processed S2:FAILED:3 S3:DELIVERED:1:processed"
show_logs "$EVENT_ID"
expect_receivers "S1 S3" "$EVENT_ID"
expect_log broker "MARK DEAD S2"

step 7 "GET /subscribers" \
  "S2 is DEAD with failures 1"
show_subscribers
expect_sub S2 DEAD 1

step 8 "Broadcast again" \
  "S2 is not attempted and not listed; S1 and S3 receive"
publish '{"mode": "broadcast", "payload": {"notice": "S2 still down"}}'
expect_report "S1:DELIVERED:1:processed S3:DELIVERED:1:processed"
show_logs "$EVENT_ID"
expect_receivers "S1 S3" "$EVENT_ID"
expect_sub S2 DEAD 1

step 9 "Restart S2 with the stored command from its pane" \
  "S2 re-registers; state ALIVE with failures reset to 0"
pid=$(pane_for S2)
start_cmd=$(tmux show-options -p -q -v -t "$pid" @start_cmd)
[[ -n $start_cmd ]] || die "S2 pane has no @start_cmd; cannot restart it"
tmux send-keys -t "$pid" -l "$start_cmd"
tmux send-keys -t "$pid" C-m
wait_state S2 ALIVE 20 || die "S2 did not re-register within 20 s; check the S2 pane"
show_subscribers
expect_sub S2 ALIVE 0
expect_log broker "REGISTER S2"
show_broker_new

step 10 "Broadcast" \
  "S1, S2 and S3 all receive"
publish '{"mode": "broadcast", "payload": {"notice": "S2 is back"}}'
expect_report "S1:DELIVERED:1:processed S2:DELIVERED:1:processed S3:DELIVERED:1:processed"
show_logs "$EVENT_ID"
expect_receivers "S1 S2 S3" "$EVENT_ID"

step 11 "Deregister S3 with DELETE /subscribe/S3, then broadcast" \
  "S3 is removed from the registry and is not in the report; S3 logs nothing"
resp=$(curl -sS -X DELETE "$BROKER/subscribe/S3") || die "DELETE /subscribe/S3 failed"
printf '    %s\n' "$resp"
printf '%s' "$resp" | python3 -c 'import json, sys; sys.exit(0 if json.load(sys.stdin).get("removed") is True else 1)' ||
  die "DELETE /subscribe/S3 did not confirm removal"
publish '{"mode": "broadcast", "payload": {"notice": "S3 has left"}}'
expect_report "S1:DELIVERED:1:processed S2:DELIVERED:1:processed"
show_logs "$EVENT_ID"
expect_receivers "S1 S2" "$EVENT_ID"
expect_state S3 missing

printf '\nDemo complete. All checks passed. S3 is still running as a process but is deregistered.\n'
printf 'Logs of this run: %s\n' "$(dirname "$(log_file broker)")"
printf 'This session cannot rerun the demo because S3 stays deregistered.\n'
printf 'Start a fresh session with ./scripts/run_all.sh, or stop everything with: tmux kill-session -t %s\n' "$SESSION"
