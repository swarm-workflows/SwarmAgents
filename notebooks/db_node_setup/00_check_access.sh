#!/usr/bin/env bash
# Step 0: verify the db node can SSH into every node in the plan
# (direct to the management IP, or via the bastion fallback).
set -euo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

banner "Step 0: SSH reachability check ($(all_nodes | wc -l | tr -d ' ') nodes)"
rm -f "$STATE_DIR"/transport/*    # force fresh probes

echo "Bastions to try (in order): $BASTION_HOSTS"
prime_bastion || true

run_nodes probe_node $(all_nodes) || true

echo
echo "Transport per node:"
unreachable=0
for n in $(all_nodes); do
    t="$(cat "$STATE_DIR/transport/$n" 2>/dev/null || echo UNREACHABLE)"
    if [ "$t" = "UNREACHABLE" ]; then
        printf "  %-12s UNREACHABLE  (%s)\n" "$n" \
            "$(grep -v '^---' "$LOG_DIR/probe-$n.err" 2>/dev/null | tail -n1)"
        unreachable=$((unreachable + 1))
    else
        printf "  %-12s %s\n" "$n" "$t"
    fi
done

if [ "$unreachable" -eq 0 ]; then
    echo; echo "All nodes reachable."
else
    echo
    echo "$unreachable nodes unreachable. Per-attempt SSH errors are in"
    echo "  $LOG_DIR/probe-<node>.err"
    echo "Re-running this step only re-probes and is cheap: ./00_check_access.sh"
    echo "If no bastion key was uploaded, add it via upload_to_db.sh (BASTION_KEY=...)."
    exit 1
fi
