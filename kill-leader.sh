#!/usr/bin/env bash
# Finds the current leader and kills it to demo failover

echo "=== Current cluster state ==="
curl -s http://localhost:3000/cluster-state | python3 -c "
import sys, json
nodes = json.load(sys.stdin)
for n in nodes:
    print(f\"  {n['id']:10s}  role={n.get('role','?'):10s}  term={n.get('term','?')}\")
"

LEADER=$(curl -s http://localhost:3000/cluster-state | python3 -c "
import sys, json
nodes = json.load(sys.stdin)
for n in nodes:
    if n.get('role') == 'leader':
        print(n['id'])
        break
")

if [ -z "$LEADER" ]; then
  echo "No leader found!"
  exit 1
fi

echo ""
echo "=== Killing leader: $LEADER ==="
docker stop "$LEADER"
echo ""
echo "=== Waiting 2 seconds for new election... ==="
sleep 2
echo ""
echo "=== New cluster state ==="
curl -s http://localhost:3000/cluster-state | python3 -c "
import sys, json
nodes = json.load(sys.stdin)
for n in nodes:
    print(f\"  {n['id']:10s}  role={n.get('role','?'):10s}  term={n.get('term','?')}\")
"
echo ""
echo "=== Restarting $LEADER as follower ==="
docker start "$LEADER"
