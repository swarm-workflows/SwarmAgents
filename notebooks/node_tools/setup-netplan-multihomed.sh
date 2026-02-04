#!/usr/bin/env bash
# Configure:
#  1) UPLINK static + conditional routing (PBR for IPv4, standard for IPv6)
#  2) LAN static + internal route
#  3) rp_filter relaxed for asymmetric routing
#
# Requires: Netplan, iproute2, python3
# Safe to re-run (idempotent) and uses /etc/netplan/ for persistence.

set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  setup-multihomed-netplan.sh [options]

Options (CLI args override env vars):
  -I <if>     UPLINK_IF          (e.g., "enp8s0")
  -A <cidr>   UPLINK_ADDR        (e.g., "203.0.113.10/28")
  -G <ip>     UPLINK_GW          (e.g., "203.0.113.1")
  -T <num>    PBR_TABLE          (default: $PBR_TABLE or 100)
  -P <num>    PBR_PRIORITY       (default: $PBR_PRIORITY or 1000)

  -i <if>     LAN_IF             (e.g., "enp9s0")
  -a <cidr>   LAN_ADDR           (e.g., "10.134.135.2/24")
  -n <cidr>   LAN_NET            (default: $LAN_NET or "10.128.0.0/10")
  -g <ip>     LAN_GW             (e.g., "10.134.135.1")

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
PBR_TABLE="${PBR_TABLE:-100}"
PBR_PRIORITY="${PBR_PRIORITY:-1000}"
NETPLAN_UPLINK_FILE="/etc/netplan/90-uplink-pbr.yaml"

LAN_IF="${LAN_IF:-}"
LAN_ADDR="${LAN_ADDR:-}"
LAN_NET="${LAN_NET:-10.128.0.0/10}"
LAN_GW="${LAN_GW:-}"
NETPLAN_LAN_FILE="/etc/netplan/91-lan-route.yaml"

VERBOSE="${VERBOSE:-1}"
NETPLAN_YAML="" # Accumulates the full Netplan config

# Track which sides were explicitly specified via CLI
CLI_SET_UPLINK=0
CLI_SET_LAN=0

# =========================
# CLI parsing
# =========================
while getopts ":I:A:G:T:P:i:a:n:g:vqh" opt; do
  case "$opt" in
    I) UPLINK_IF="$OPTARG"; CLI_SET_UPLINK=1 ;;
    A) UPLINK_ADDR="$OPTARG" ;;
    G) UPLINK_GW="$OPTARG" ;;
    T) PBR_TABLE="$OPTARG" ;;
    P) PBR_PRIORITY="$OPTARG" ;;
    i) LAN_IF="$OPTARG"; CLI_SET_LAN=1 ;;
    a) LAN_ADDR="$OPTARG" ;;
    n) LAN_NET="$OPTARG" ;;
    g) LAN_GW="$OPTARG" ;;
    v) VERBOSE=1 ;;
    q) VERBOSE=0 ;;
    h) usage; exit 0 ;;
    \?) echo "[!] Invalid option: -$OPTARG" >&2; usage; exit 2 ;;
    :)  echo "[!] Option -$OPTARG requires an argument." >&2; usage; exit 2 ;;
  esac
done
shift $((OPTIND-1))

# Implicit side selection:
DO_UPLINK=$(( CLI_SET_UPLINK == 1 || (CLI_SET_UPLINK == 0 && CLI_SET_LAN == 0) ? 1 : 0 ))
DO_LAN=$(( CLI_SET_LAN == 1 || (CLI_SET_UPLINK == 0 && CLI_SET_LAN == 0) ? 1 : 0 ))

log(){ [ "$VERBOSE" = "1" ] && echo "[*] $*" >&2 || true; }
die(){ echo "[!] $*" >&2; exit 1; }
need(){ command -v "$1" >/dev/null 2>&1 || die "Missing command: $1"; }

need netplan
need ip

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
print(hosts[0] if hosts else str(net.network_address)+"/"+str(net.prefixlen))
PY
}

# ==========================================
# IP Type Detection for Conditional Routing
# ==========================================
man_ip_type=ipv4
log "Detecting primary WAN IP connectivity..."

if command -v ping6 >/dev/null 2>&1; then
    # Use ping6 directly if it exists
    ping6 -c 1 -W 1 google.com &>/dev/null && man_ip_type=ipv6
elif command -v ping >/dev/null 2>&1; then
    # Try 'ping -6' if ping6 command doesn't exist but ping does
    ping -6 -c 1 -W 1 google.com &>/dev/null && man_ip_type=ipv6
fi

log "Detected main IP type: $man_ip_type"

# ======================
# 1) UPLINK (WAN) - Conditional Configuration
# ======================
if [ "$DO_UPLINK" -eq 1 ]; then
  [ -n "${UPLINK_IF:-}" ] || die "Provide -I <uplink-if> to configure UPLINK"

  if [ -z "$UPLINK_ADDR" ]; then
    UPLINK_ADDR="$(get_ip_cidr "$UPLINK_IF" || true)"
    [ -n "$UPLINK_ADDR" ] || die "UPLINK_ADDR not set and could not detect IPv4 on $UPLINK_IF"
  fi
  UPLINK_IP="$(cidr_ip "$UPLINK_ADDR")"

  if [ -z "$UPLINK_GW" ]; then
    UPLINK_GW="$(ip route show | awk -v IF="$UPLINK_IF" '$0 ~ (" dev "IF" ") && $0 ~ / via / {print $3; exit}')"
    [ -n "$UPLINK_GW" ] || { UPLINK_GW="$(guess_gw_from_cidr "$UPLINK_ADDR")"; log "Guessed UPLINK_GW=$UPLINK_GW from $UPLINK_ADDR"; }
  fi

  log "UPLINK configuration: if=$UPLINK_IF addr=$UPLINK_ADDR gw=$UPLINK_GW"

  if [ "$man_ip_type" = "ipv6" ]; then
    log "IPv6 detected. Configuring UPLINK with a standard default route (no PBR)."
    
    # Generate Netplan YAML for standard default route
    NETPLAN_YAML_UPLINK=$(cat <<YAML
network:
  version: 2
  renderer: networkd
  ethernets:
    $UPLINK_IF:
      dhcp4: no
      addresses:
        - $UPLINK_ADDR
      routes:
        - to: 0.0.0.0/0
          via: $UPLINK_GW
YAML
)
    log "Writing Netplan config to $NETPLAN_UPLINK_FILE"
    echo "$NETPLAN_YAML_UPLINK" | sudo tee "$NETPLAN_UPLINK_FILE" > /dev/null

    # Apply chmod 600
    sudo chmod 600 "$NETPLAN_UPLINK_FILE"
    log "Set permissions on $NETPLAN_UPLINK_FILE to 600."

    # Remove the PBR rule if it exists from a previous run
    log "Removing PBR rule for $UPLINK_IP."
    sudo ip rule del from "$UPLINK_IP"/32 table "$PBR_TABLE" 2>/dev/null || true

  else
    log "IPv4 detected. Configuring UPLINK with Policy-Based Routing (Table $PBR_TABLE)."
    
    # Generate Netplan YAML for PBR (routes into custom table)
    NETPLAN_YAML_UPLINK=$(cat <<YAML
network:
  version: 2
  renderer: networkd
  ethernets:
    $UPLINK_IF:
      dhcp4: no
      addresses:
        - $UPLINK_ADDR
      routes:
        - to: 0.0.0.0/0
          via: $UPLINK_GW
          table: $PBR_TABLE
YAML
)
    log "Writing Netplan config to $NETPLAN_UPLINK_FILE"
    echo "$NETPLAN_YAML_UPLINK" | sudo tee "$NETPLAN_UPLINK_FILE" > /dev/null

    # Apply chmod 600
    sudo chmod 600 "$NETPLAN_UPLINK_FILE"
    log "Set permissions on $NETPLAN_UPLINK_FILE to 600."

    # Add the Policy Routing Rule (ip rule)
    log "Setting Policy Routing Rule (ip rule from $UPLINK_IP to table $PBR_TABLE)"
    sudo ip rule del from "$UPLINK_IP"/32 table "$PBR_TABLE" 2>/dev/null || true
    sudo ip rule add from "$UPLINK_IP"/32 table "$PBR_TABLE" priority "$PBR_PRIORITY"

  fi
fi

# =======================
# 2) LAN (internal) - Static IP and Internal Route
# =======================
if [ "$DO_LAN" -eq 1 ]; then
  [ -n "${LAN_IF:-}" ] || die "Provide -i <lan-if> to configure LAN"
  
  if [ -z "$LAN_ADDR" ]; then
    LAN_ADDR="$(get_ip_cidr "$LAN_IF" || true)"
    [ -n "$LAN_ADDR" ] || die "LAN_ADDR not set and could not detect IPv4 on $LAN_IF"
    log "Detected LAN_ADDR=$LAN_ADDR on $LAN_IF."
  fi

  if [ -z "$LAN_GW" ]; then
    LAN_GW="$(ip route show | awk -v IF="$LAN_IF" '$0 ~ (" dev "IF" ") && $0 ~ / via / {print $3; exit}')"
    [ -n "$LAN_GW" ] || { LAN_GW="$(guess_gw_from_cidr "$LAN_ADDR")"; log "Guessed LAN_GW=$LAN_GW from $LAN_ADDR"; }
  fi

  log "LAN (Netplan): if=$LAN_IF addr=$LAN_ADDR net=$LAN_NET gw=$LAN_GW"

  # Generate Netplan YAML for the LAN interface
  NETPLAN_YAML_LAN=$(cat <<YAML
network:
  version: 2
  renderer: networkd
  ethernets:
    $LAN_IF:
      dhcp4: no
      addresses:
        - $LAN_ADDR
      routes:
        # Add the route for the internal LAN network to the LAN gateway
        - to: $LAN_NET
          via: $LAN_GW
YAML
)

  log "Writing Netplan config to $NETPLAN_LAN_FILE"
  echo "$NETPLAN_YAML_LAN" | sudo tee "$NETPLAN_LAN_FILE" > /dev/null

  # Apply chmod 600
  sudo chmod 600 "$NETPLAN_LAN_FILE"
  log "Set permissions on $NETPLAN_LAN_FILE to 600."
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

# ==========================================
# 4) Apply and Verify
# ==========================================
log "Applying Netplan configuration..."
# Use netplan generate/apply in separate steps to catch errors early
sudo netplan generate || die "Netplan syntax check failed. Check YAML files."
sudo netplan apply || die "Netplan failed to apply configuration."

echo
log "Effective rules:"
ip rule show | sed 's/^/  /'

echo
if [ "$DO_UPLINK" -eq 1 ] && [ "$man_ip_type" != "ipv6" ]; then
  log "Routes in policy table $PBR_TABLE (PBR Mode):"
  ip route show table "$PBR_TABLE" | sed 's/^/  /'
fi

echo
log "Main routing table highlights:"
ip route show | sed 's/^/  /' | head -n 50

echo
if [ "$DO_UPLINK" -eq 1 ] && [ -n "${UPLINK_ADDR:-}" ]; then
  log "Done. Services bound to $UPLINK_IP will reply via $UPLINK_IF."
  if [ "$man_ip_type" != "ipv6" ]; then
    log "  (Using Policy-Based Routing via table $PBR_TABLE for source $UPLINK_IP)."
  else
    log "  (Using Standard Routing for source $UPLINK_IP)."
  fi
else
  log "Done."
fi
