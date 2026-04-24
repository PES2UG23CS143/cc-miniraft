#!/usr/bin/env bash
# Print live cluster state every second

watch -n 1 'curl -s http://localhost:3000/cluster-state | python3 -c "
import sys, json
nodes = json.load(sys.stdin)
print(\"ID          ROLE        TERM  LOG   COMMIT\")
print(\"-\" * 48)
for n in nodes:
    print(f\"{n.get(chr(105)+chr(100),chr(63)):11s} {n.get(chr(114)+chr(111)+chr(108)+chr(101),chr(63)):11s} {str(n.get(chr(116)+chr(101)+chr(114)+chr(109),chr(63))):5s} {str(n.get(chr(108)+chr(111)+chr(103)+chr(76)+chr(101)+chr(110)+chr(103)+chr(116)+chr(104),chr(63))):5s} {str(n.get(chr(99)+chr(111)+chr(109)+chr(109)+chr(105)+chr(116)+chr(73)+chr(110)+chr(100)+chr(101)+chr(120),chr(63)))}\")
"'
