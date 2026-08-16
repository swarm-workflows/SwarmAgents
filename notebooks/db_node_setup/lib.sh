#!/usr/bin/env bash
# lib.sh -- shared helpers for the on-database-node setup scripts.
# Sourced by the numbered step scripts; not meant to be executed directly.
#
# Layout on the db node (created by upload_to_db.sh):
#   ~/swarm-setup/
#     db_node_setup/        these scripts + plan/ + keys/
#     node_tools/           netplan helper scripts
#     push_swarmagents.sh
#     Prometheus_Grafana_Monitor/{tools,build}
#
# SSH transport: direct to each node's management IP with the slice key,
# falling back to a ProxyJump through the FABRIC bastion (if keys/bastion_key
# exists) for nodes the db node cannot reach directly (e.g. IPv6-only sites
# from an IPv4-only db node). The chosen transport is probed once per node
# and cached in state/transport/.

set -euo pipefail

SETUP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SETUP_DIR")"
PLAN_DIR="$SETUP_DIR/plan"
KEY_DIR="$SETUP_DIR/keys"
STATE_DIR="$SETUP_DIR/state"
LOG_DIR="$SETUP_DIR/logs"
SLICE_KEY="${SLICE_KEY:-$KEY_DIR/slice_key}"
BASTION_KEY="${BASTION_KEY:-$KEY_DIR/bastion_key}"

# When driven from the machine that runs the notebook (rather than from the
# database node, where upload_to_db.sh stages copies into keys/), the keys are
# wherever fablib keeps them. gen_inventory.py recorded those paths, so fall
# back to them instead of making the user copy keys around.
if [ ! -f "$SLICE_KEY" ] && [ -f "$PLAN_DIR/upload.env" ]; then
    # shellcheck disable=SC1091
    . "$PLAN_DIR/upload.env"
    [ -f "${SLICE_KEY_PATH:-}" ]   && SLICE_KEY="$SLICE_KEY_PATH"
    [ -f "${BASTION_KEY_PATH:-}" ] && BASTION_KEY="$BASTION_KEY_PATH"
fi
PARALLEL="${PARALLEL:-16}"

mkdir -p "$STATE_DIR/transport" "$STATE_DIR/status" "$LOG_DIR"

[ -f "$PLAN_DIR/nodes.tsv" ] || { echo "ERROR: $PLAN_DIR/nodes.tsv missing"; exit 1; }
[ -f "$SLICE_KEY" ] || { echo "ERROR: slice key missing at $SLICE_KEY"; exit 1; }

# config.env: BRANCH, DB_NODE, MONITOR_NODE, AGENT_COUNT
. "$PLAN_DIR/config.env"
# bastion.env: BASTION_HOST, BASTION_USER
if [ -f "$PLAN_DIR/bastion.env" ]; then . "$PLAN_DIR/bastion.env"; fi

# ---------------------------------------------------------------------------
# Node lists / lookups (nodes.tsv: name  user  mgmt_ip  swarm  site)
# ---------------------------------------------------------------------------
all_nodes()   { awk -F'\t' '{print $1}' "$PLAN_DIR/nodes.tsv"; }
swarm_nodes() { awk -F'\t' '$4==1{print $1}' "$PLAN_DIR/nodes.tsv"; }
agent_nodes() { awk -F'\t' '$1 ~ /^agent-/{print $1}' "$PLAN_DIR/nodes.tsv"; }
node_user_ip(){ awk -F'\t' -v n="$1" '$1==n{print $2, $3; exit}' "$PLAN_DIR/nodes.tsv"; }

# ---------------------------------------------------------------------------
# SSH transport
# ---------------------------------------------------------------------------
SSH_OPTS=(-i "$SLICE_KEY"
          -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null
          -o BatchMode=yes -o ConnectTimeout=20
          -o ServerAliveInterval=15 -o LogLevel=ERROR)

# FABRIC runs SEVERAL bastion hosts and they do not all reach every site --
# e.g. HAWI is reachable via bastion-star-1 but not via the default bastion.
# So each node is probed against every bastion in turn and the one that
# works is remembered per node.
#
# Order matters: the first entry is tried first. Override the whole list with
#   BASTION_HOSTS="host-a host-b" ./00_check_access.sh
BASTION_HOSTS="${BASTION_HOSTS:-${BASTION_HOST:-bastion.fabric-testbed.net} bastion-star-1.fabric-testbed.net bastion-star-2.fabric-testbed.net}"

# Deduplicate while preserving order
BASTION_HOSTS="$(printf '%s\n' $BASTION_HOSTS | awk '!seen[$0]++' | tr '\n' ' ')"

# Each bastion gets its own multiplexed master connection (ControlMaster),
# so a 94-node fan-out is N channels over a few sessions rather than N
# separate handshakes the bastion would rate-limit.
#
# ControlPath sockets live under a SHORT directory: Unix domain socket paths
# are capped at ~104 bytes, and a path under $STATE_DIR overflows it
# ("ControlPath too long"). Index the bastion rather than name it to stay short.
CM_DIR="${CM_DIR:-/tmp/swm-cm-$(id -u)}"
mkdir -p "$CM_DIR" 2>/dev/null || true
chmod 700 "$CM_DIR" 2>/dev/null || true

# Name the socket from a checksum of the hostname, NOT its position in
# BASTION_HOSTS -- prime_bastion prunes that list, and a positional name
# would then point at a different bastion's master.
_bastion_sock() {
    printf '%s/b%s.sock' "$CM_DIR" "$(printf '%s' "$1" | cksum | awk '{print $1}')"
}

# -W target spec. IPv6 literals MUST be bracketed -- `-W 2620:...:cc7f:22`
# is unparseable ("Bad stdio forwarding specification") because the colons
# are ambiguous. Most FABRIC management addresses are IPv6, so this is the
# common path, not an edge case.
_wspec() {
    case "$1" in
        *:*) printf '[%s]:22' "$1" ;;   # IPv6 literal
        *)   printf '%s:22'   "$1" ;;   # IPv4
    esac
}

# _bastion_proxy <target-ip> <bastion-host>
_bastion_proxy() {
    printf 'ProxyCommand=ssh -i %s -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o BatchMode=yes -o LogLevel=ERROR -o ConnectTimeout=15 -o ControlMaster=auto -o ControlPath=%s -o ControlPersist=10m -o ServerAliveInterval=15 -W %s %s@%s' \
        "$BASTION_KEY" "$(_bastion_sock "$2")" "$(_wspec "$1")" "$BASTION_USER" "$2"
}

bastion_available() { [ -f "$BASTION_KEY" ] && [ -n "${BASTION_USER:-}" ]; }

# Open a master connection to every bastion up front (avoids a thundering
# herd of parallel ssh processes racing to become the master).
# NOTE: FABRIC bastion accounts have no shell ("This account is currently
# not available"), so masters are opened with -N (no remote command); only
# TCP forwarding (-W) is permitted on them.
prime_bastion() {
    bastion_available || return 0
    local bh sock up_hosts=""
    for bh in $BASTION_HOSTS; do
        sock="$(_bastion_sock "$bh")"
        ssh -fN -i "$BASTION_KEY" -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
            -o BatchMode=yes -o LogLevel=ERROR -o ConnectTimeout=30 \
            -o ControlMaster=auto -o ControlPath="$sock" -o ControlPersist=10m \
            -o ServerAliveInterval=15 \
            "$BASTION_USER@$bh" </dev/null >>"$LOG_DIR/bastion-master.log" 2>&1 || true
        if ssh -O check -o ControlPath="$sock" "$BASTION_USER@$bh" >/dev/null 2>&1; then
            echo "  bastion up: $bh"
            up_hosts="$up_hosts $bh"
        else
            echo "  bastion unavailable (skipping): $bh" >&2
        fi
    done
    if [ -n "$up_hosts" ]; then
        # Probe only against bastions that actually answered -- a dead host
        # would otherwise burn its full timeout budget on every node.
        BASTION_HOSTS="${up_hosts# }"
        return 0
    fi
    echo "WARNING: no bastion master; hops will connect individually" >&2
    return 1
}

# Probe once which transport reaches a node; cache in state/transport/<node>
# as either "direct" or "bastion:<bastion-host>".
# Errors from every attempt land in logs/probe-<node>.err for diagnosis.
probe_node() {
    local n="$1" user ip tf attempt bh
    read -r user ip <<<"$(node_user_ip "$n")"
    [ -n "${ip:-}" ] || { echo "unknown node: $n" >&2; return 1; }
    tf="$STATE_DIR/transport/$n"
    local err="$LOG_DIR/probe-$n.err"
    : >"$err"

    echo "--- direct $user@$ip ---" >>"$err"
    if ssh "${SSH_OPTS[@]}" -o ConnectTimeout=8 "$user@$ip" true </dev/null >/dev/null 2>>"$err"; then
        echo direct >"$tf"; return 0
    fi

    if bastion_available; then
        for bh in $BASTION_HOSTS; do
            for attempt in 1 2; do
                echo "--- bastion $bh attempt $attempt ---" >>"$err"
                if ssh "${SSH_OPTS[@]}" -o ConnectTimeout=45 -o "$(_bastion_proxy "$ip" "$bh")" \
                       "$user@$ip" true </dev/null >/dev/null 2>>"$err"; then
                    echo "bastion:$bh" >"$tf"; return 0
                fi
                sleep $((RANDOM % 4 + 1))
            done
        done
    fi
    rm -f "$tf"
    echo "UNREACHABLE: $n ($user@$ip) -- $(grep -v '^---' "$err" | tail -n1)" >&2
    return 1
}

# nssh_in <node> <cmd...>  -- ssh keeping the caller's stdin (for piping data)
nssh_in() {
    local n="$1"; shift
    local user ip tf
    read -r user ip <<<"$(node_user_ip "$n")"
    [ -n "${ip:-}" ] || { echo "unknown node: $n" >&2; return 1; }
    tf="$STATE_DIR/transport/$n"
    [ -f "$tf" ] || probe_node "$n" >/dev/null || return 1
    local transport bh
    transport="$(cat "$tf")"
    case "$transport" in
        bastion:*)
            bh="${transport#bastion:}"
            ssh "${SSH_OPTS[@]}" -o "$(_bastion_proxy "$ip" "$bh")" "$user@$ip" "$@"
            ;;
        bastion)   # legacy state from an older run: use the first bastion
            bh="${BASTION_HOSTS%% *}"
            ssh "${SSH_OPTS[@]}" -o "$(_bastion_proxy "$ip" "$bh")" "$user@$ip" "$@"
            ;;
        *)
            ssh "${SSH_OPTS[@]}" "$user@$ip" "$@"
            ;;
    esac
}

# nssh <node> <cmd...>  -- ssh with stdin closed (safe inside read loops)
nssh() { local n="$1"; shift; nssh_in "$n" "$@" </dev/null; }

# npush <local-path> <node> <remote-path>  -- copy one file (cat over ssh;
# avoids scp's IPv6/bastion quirks)
npush() { nssh_in "$2" "cat > '$3'" <"$1"; }

# npush_dir <local-dir> <node>  -- tar a directory into the remote $HOME
npush_dir() {
    local dir="$1" n="$2"
    tar -C "$(dirname "$dir")" -cz "$(basename "$dir")" | nssh_in "$n" "tar -xz"
}

# ---------------------------------------------------------------------------
# Parallel runner: run_nodes <func> <node>...
# Runs <func> <node> for each node, $PARALLEL at a time, logging to
# logs/<func>-<node>.log. Prints failures and returns non-zero if any failed.
# ---------------------------------------------------------------------------
run_nodes() {
    local func="$1"; shift
    local n count=0
    for n in "$@"; do
        (
            if "$func" "$n" >"$LOG_DIR/${func}-${n}.log" 2>&1; then
                echo ok >"$STATE_DIR/status/${func}-${n}"
            else
                echo fail >"$STATE_DIR/status/${func}-${n}"
            fi
        ) &
        count=$((count + 1))
        if [ $((count % PARALLEL)) -eq 0 ]; then wait; fi
    done
    wait
    local fails=0
    for n in "$@"; do
        if [ "$(cat "$STATE_DIR/status/${func}-${n}" 2>/dev/null)" != "ok" ]; then
            echo "  [FAIL] $func $n  (see $LOG_DIR/${func}-${n}.log)" >&2
            fails=$((fails + 1))
        fi
    done
    if [ "$fails" -gt 0 ]; then
        echo "$func: $fails/$# nodes FAILED" >&2
        return 1
    fi
    echo "$func: all $# nodes OK"
}

banner() { echo; echo "============================================================"; echo "== $*"; echo "============================================================"; }
