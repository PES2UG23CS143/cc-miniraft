#!/usr/bin/env bash
# Blue-Green Rolling Replica Replacement
# Replaces each replica one at a time with zero client downtime.
# Usage:  ./blue-green-swap.sh [replica1|replica2|replica3|all]
#
# What it does per replica:
#   1. Stops the target container (triggers RAFT election on survivors).
#   2. Waits for a new leader to be elected.
#   3. Restarts the container — new instance joins as follower and catches up.
#   4. Waits until it is in sync before moving to the next replica.

set -euo pipefail

GW="http://localhost:3000"
REPLICAS=(replica1 replica2 replica3)
TARGET="${1:-all}"

log()  { echo "[$(date '+%H:%M:%S')] $*"; }
pass() { echo "[$(date '+%H:%M:%S')] ✔  $*"; }
fail() { echo "[$(date '+%H:%M:%S')] ✘  $*" >&2; exit 1; }

# ── helpers ──────────────────────────────────────────────────────────────────
cluster_state() {
  curl -s --max-time 2 "${GW}/cluster-state" 2>/dev/null || echo "[]"
}

current_leader() {
  cluster_state | python3 -c "
import sys, json
nodes = json.load(sys.stdin)
for n in nodes:
    if n.get('role') == 'leader':
        print(n['id'])
        break
" 2>/dev/null || true
}

wait_for_leader() {
  local tries=0
  log "Waiting for leader election..."
  while [ $tries -lt 20 ]; do
    local leader
    leader=$(current_leader)
    if [ -n "$leader" ]; then
      pass "Leader elected: $leader"
      return 0
    fi
    sleep 0.5
    tries=$((tries + 1))
  done
  fail "No leader elected after 10 s"
}

wait_for_sync() {
  local node="$1"
  local tries=0
  log "Waiting for ${node} to catch up..."
  while [ $tries -lt 30 ]; do
    local state
    state=$(cluster_state)
    local node_commit leader_commit
    node_commit=$(echo "$state" | python3 -c "
import sys, json
nodes = json.load(sys.stdin)
for n in nodes:
    if n.get('id') == '${node}':
        print(n.get('commitIndex', -1))
        break
" 2>/dev/null || echo "-2")
    leader_commit=$(echo "$state" | python3 -c "
import sys, json
nodes = json.load(sys.stdin)
for n in nodes:
    if n.get('role') == 'leader':
        print(n.get('commitIndex', -1))
        break
" 2>/dev/null || echo "-2")
    if [ "$node_commit" != "-2" ] && [ "$node_commit" = "$leader_commit" ]; then
      pass "${node} is in sync (commitIndex=${node_commit})"
      return 0
    fi
    sleep 0.5
    tries=$((tries + 1))
  done
  fail "${node} failed to sync within 15 s"
}

# ── swap one replica ──────────────────────────────────────────────────────────
swap_replica() {
  local replica="$1"
  log "━━━ Blue-green swap: ${replica} ━━━"

  log "Stopping ${replica}..."
  docker stop "$replica" >/dev/null
  sleep 0.3

  wait_for_leader

  log "Restarting ${replica}..."
  docker start "$replica" >/dev/null

  wait_for_sync "$replica"
  pass "${replica} swap complete — system healthy"
  echo ""
}

# ── main ─────────────────────────────────────────────────────────────────────
echo ""
echo "════════════════════════════════════════"
echo "  Mini-RAFT Blue-Green Rolling Swap"
echo "════════════════════════════════════════"
echo ""

# Print current state
log "Current cluster state:"
cluster_state | python3 -c "
import sys, json
nodes = json.load(sys.stdin)
print('  {:<12} {:<12} {:<6} {:<6} {}'.format('ID','ROLE','TERM','LOG','COMMIT'))
print('  ' + '-'*48)
for n in nodes:
    print('  {:<12} {:<12} {:<6} {:<6} {}'.format(
        n.get('id','?'), n.get('role','?'),
        str(n.get('term','?')), str(n.get('logLength','?')),
        str(n.get('commitIndex','?'))
    ))
" 2>/dev/null || true
echo ""

if [ "$TARGET" = "all" ]; then
  for r in "${REPLICAS[@]}"; do
    swap_replica "$r"
  done
  pass "All replicas replaced — zero downtime rolling swap complete!"
else
  # Validate target
  valid=false
  for r in "${REPLICAS[@]}"; do
    [ "$r" = "$TARGET" ] && valid=true && break
  done
  $valid || fail "Unknown target '${TARGET}'. Use: replica1 | replica2 | replica3 | all"
  swap_replica "$TARGET"
fi
