#!/usr/bin/env bash
# Configure:
#  1) UPLINK static + policy routing (table 100, src-based rule)
#  2) LAN static + internal route
#  3) rp_filter relaxed for asymmetric routing
#
# Requires: NetworkManager (nmcli)
# Safe to re-run (idempotent).

set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  setup-multihomed.sh [options]

Options (CLI args override env vars):
  -I <if>     UPLINK_IF              (e.g., "enp8s0")
  -A <cidr>   UPLINK_ADDR            (e.g., "203.0.113.10/28")
  -G <ip>     UPLINK_GW              (e.g., "203.0.113.1")
  -N <name>   UPLINK_CONN            (default: $UPLINK_CONN or "uplink-static")
  -T <num>    PBR_TABLE              (default: $PBR_TABLE or 100)
  -P <num>    PBR_PRIORITY           (default: $PBR_PRIORITY or 1000)

  -i <if>     LAN_IF                 (e.g., "enp9s0")
  -a <cidr>   LAN_ADDR               (e.g., "10.134.135.2/24")
  -n <cidr>   LAN_NET                (default: $LAN_NET or "10.128.0.0/10")
  -g <ip>     LAN_GW                 (e.g., "10.134.135.1")
  -c <name>   LAN_CONN               (default: $LAN_CONN or "lan-static")

  -v          VERBOSE=1 (default)
  -q          VERBOSE=0
  -h          Show this help and exit

Behavior:
- If you pass -I (uplink) and/or -i (lan), the script configures ONLY those sides.
- If neither -I nor -i is provided, it configures BOTH sides (backward compatible).
USAGE
}

# =========================
# Defaults (override via env or CLI)
# =========================
UPLINK_IF="${UPLINK_IF:-}"
UPLINK_ADDR="${UPLINK_ADDR:-}"
UPLINK_GW="${UPLINK_GW:-}"
UPLINK_CONN="${UPLINK_CONN:-uplink-static}"
PBR_TABLE="${PBR_TABLE:-100}"
PBR_PRIORITY="${PBR_PRIORITY:-1000}"

LAN_IF="${LAN_IF:-}"
LAN_ADDR="${LAN_ADDR:-}"
LAN_NET="${LAN_NET:-10.128.0.0/10}"
LAN_GW="${LAN_GW:-}"
LAN_CONN="${LAN_CONN:-lan-static}"

VERBOSE="${VERBOSE:-1}"

# Track which sides were explicitly specified via CLI
CLI_SET_UPLINK=0
CLI_SET_LAN=0

# =========================
# CLI parsing
# =========================
while getopts ":I:A:G:N:T:P:i:a:n:g:c:vqh" opt; do
  case "$opt" in
    I) UPLINK_IF="$OPTARG"; CLI_SET_UPLINK=1 ;;
    A) UPLINK_ADDR="$OPTARG" ;;
    G) UPLINK_GW="$OPTARG" ;;
    N) UPLINK_CONN="$OPTARG" ;;
    T) PBR_TABLE="$OPTARG" ;;
    P) PBR_PRIORITY="$OPTARG" ;;
    i) LAN_IF="$OPTARG"; CLI_SET_LAN=1 ;;
    a) LAN_ADDR="$OPTARG" ;;
    n) LAN_NET="$OPTARG" ;;
    g) LAN_GW="$OPTARG" ;;
    c) LAN_CONN="$OPTARG" ;;
    v) VERBOSE=1 ;;
    q) VERBOSE=0 ;;
    h) usage; exit 0 ;;
    \?) echo "[!] Invalid option: -$OPTARG" >&2; usage; exit 2 ;;
    :)  echo "[!] Option -$OPTARG requires an argument." >&2; usage; exit 2 ;;
  esac
done
shift $((OPTIND-1))

# Implicit side selection:
# - If any side was specified via CLI (-I and/or -i), do ONLY those.
# - If neither was specified, do both (back-compat).
DO_UPLINK=$(( CLI_SET_UPLINK == 1 || (CLI_SET_UPLINK == 0 && CLI_SET_LAN == 0) ? 1 : 0 ))
DO_LAN=$(( CLI_SET_LAN == 1 || (CLI_SET_UPLINK == 0 && CLI_SET_LAN == 0) ? 1 : 0 ))

log(){ [ "$VERBOSE" = "1" ] && echo "[*] $*" >&2 || true; }
die(){ echo "[!] $*" >&2; exit 1; }
need(){ command -v "$1" >/dev/null 2>&1 || die "Missing command: $1"; }

need nmcli
need ip

nm() { [ "$VERBOSE" = "1" ] && echo "+ nmcli $*" >&2; nmcli "$@"; }

# Helpers
get_ip_cidr(){ ip -4 -o addr show dev "$1" | awk '{print $4}' | head -n1; }
cidr_ip(){ echo "${1%/*}"; }
cidr_prefix(){ echo "${1#*/}"; }
net_base_from_cidr(){ python3 - "$1" <<'PY'
import sys, ipaddress
print(ipaddress.ip_network(sys.argv[1], strict=False).network_address)
PY
}
guess_gw_from_cidr(){ python3 - "$1" <<'PY'
import sys, ipaddress
net = ipaddress.ip_network(sys.argv[1], strict=False)
hosts = list(net.hosts())
print(hosts[0] if hosts else net.network_address+1)
PY
}
conn_exists(){ nm -t -f NAME c show | awk '{print $1}' | grep -Fxq "$1"; }

# ======================
# 1) UPLINK (WAN)
# ======================
if [ "$DO_UPLINK" -eq 1 ]; then
  [ -n "${UPLINK_IF:-}" ] || die "Provide -I <uplink-if> to configure UPLINK"
  if [ -z "$UPLINK_ADDR" ]; then
    UPLINK_ADDR="$(get_ip_cidr "$UPLINK_IF" || true)"
    [ -n "$UPLINK_ADDR" ] || die "UPLINK_ADDR not set and could not detect IPv4 on $UPLINK_IF"
  fi
  UPLINK_IP="$(cidr_ip "$UPLINK_ADDR")"
  UPLINK_PREFIX="$(cidr_prefix "$UPLINK_ADDR")"
  UPLINK_SUBNET="$(net_base_from_cidr "$UPLINK_ADDR")/$UPLINK_PREFIX"

  if [ -z "$UPLINK_GW" ]; then
    UPLINK_GW="$(ip route show | awk -v IF="$UPLINK_IF" '$0 ~ (" dev "IF" ") && $0 ~ / via / {print $3; exit}')"
    [ -n "$UPLINK_GW" ] || { UPLINK_GW="$(guess_gw_from_cidr "$UPLINK_ADDR")"; log "Guessed UPLINK_GW=$UPLINK_GW from $UPLINK_ADDR"; }
  fi

  log "UPLINK: if=$UPLINK_IF addr=$UPLINK_ADDR gw=$UPLINK_GW subnet=$UPLINK_SUBNET"

  if ! conn_exists "$UPLINK_CONN"; then
    nm c add type ethernet ifname "$UPLINK_IF" con-name "$UPLINK_CONN" \
      ipv4.method manual ipv4.addresses "$UPLINK_ADDR" connection.autoconnect yes
  else
    nm c mod "$UPLINK_CONN" ipv4.method manual ipv4.addresses "$UPLINK_ADDR" connection.autoconnect yes
  fi

  nm c mod "$UPLINK_CONN" ipv4.never-default yes
  nm c mod "$UPLINK_CONN" ipv4.route-table "$PBR_TABLE"
  nm c mod "$UPLINK_CONN" +ipv4.routes "$UPLINK_SUBNET 0.0.0.0"
  nm c mod "$UPLINK_CONN" +ipv4.routes "0.0.0.0/0 $UPLINK_GW"
  nm c mod "$UPLINK_CONN" +ipv4.routing-rules "priority $PBR_PRIORITY from $UPLINK_IP/32 table $PBR_TABLE"
  nm c mod "$UPLINK_CONN" ipv4.route-metric 200
  nm c up "$UPLINK_CONN" || nm c up "$UPLINK_CONN"
fi

# =======================
# 2) LAN (internal)
# =======================
if [ "$DO_LAN" -eq 1 ]; then
  [ -n "${LAN_IF:-}" ] || die "Provide -i <lan-if> to configure LAN"
  if [ -z "$LAN_ADDR" ]; then
    LAN_ADDR="$(get_ip_cidr "$LAN_IF" || true)"
    [ -n "$LAN_ADDR" ] || die "LAN_ADDR not set and could not detect IPv4 on $LAN_IF"
  fi
  LAN_IP="$(cidr_ip "$LAN_ADDR")"

  if [ -z "$LAN_GW" ]; then
    LAN_GW="$(ip route show | awk -v IF="$LAN_IF" '$0 ~ (" dev "IF" ") && $0 ~ / via / {print $3; exit}')"
    [ -n "$LAN_GW" ] || { LAN_GW="$(guess_gw_from_cidr "$LAN_ADDR")"; log "Guessed LAN_GW=$LAN_GW from $LAN_ADDR"; }
  fi

  log "LAN: if=$LAN_IF addr=$LAN_ADDR net=$LAN_NET gw=$LAN_GW"

  if ! conn_exists "$LAN_CONN"; then
    nm c add type ethernet ifname "$LAN_IF" con-name "$LAN_CONN" \
      ipv4.method manual ipv4.addresses "$LAN_ADDR" connection.autoconnect yes
  else
    nm c mod "$LAN_CONN" ipv4.method manual ipv4.addresses "$LAN_ADDR" connection.autoconnect yes
  fi

  nm c mod "$LAN_CONN" ipv4.never-default yes
  nm c mod "$LAN_CONN" +ipv4.routes "$LAN_NET $LAN_GW"
  nm c up "$LAN_CONN" || nm c up "$LAN_CONN"
fi

# ==========================================
# 3) rp_filter (asymmetric routing tolerant)
# ==========================================
# Apply rp_filter to any interface that was touched (or both if defaulted).
SYSCTL_FILE="/etc/sysctl.d/99-multihomed.conf"
sudo mkdir -p /etc/sysctl.d
{
  echo "net.ipv4.conf.all.rp_filter=2"
  [ "$DO_UPLINK" -eq 1 ] && echo "net.ipv4.conf.$UPLINK_IF.rp_filter=2"
  [ "$DO_LAN" -eq 1 ] && echo "net.ipv4.conf.$LAN_IF.rp_filter=2"
} | sudo tee "$SYSCTL_FILE" >/dev/null
sudo sysctl --system >/dev/null

# =========
# Show info
# =========
echo
log "Effective rules:"
ip rule show | sed 's/^/  /'

echo
if [ "$DO_UPLINK" -eq 1 ]; then
  log "Routes in policy table $PBR_TABLE:"
  ip route show table "$PBR_TABLE" | sed 's/^/  /'
fi

echo
log "Main routing table highlights:"
ip route show | sed 's/^/  /' | head -n 50

echo
if [ "$DO_UPLINK" -eq 1 ] && [ -n "${UPLINK_ADDR:-}" ]; then
  log "Done. Services bound to $(cidr_ip "$UPLINK_ADDR") will reply via $UPLINK_IF (table $PBR_TABLE)."
else
  log "Done."
fi

