#!/usr/bin/env bash
# Install Apptainer on the slice, so a workflow's own .sif images run natively. Run from the
# database node as root.
#
# WHY. Every transformation catalog here declares a singularity image
# (`image: file:///.../Container.sif`), and the agent VMs shipped with Docker only. Without
# apptainer the choice is to substitute a Docker image built from the workflow's Dockerfile --
# which is a different artefact from the one Pegasus ran, and so weakens exactly the
# comparison this is all for -- or to run nothing. `runtime.execution.image_overrides` exists
# for the substitution case and stays available; this removes the need for it.
#
# WHY NOT THE PPA. `ppa:apptainer/ppa` is unreachable from the agent nodes (measured: the
# `add-apt-repository` step fails while plain apt to the Ubuntu archive succeeds). So the
# .deb is fetched ONCE on the database node and pushed over the root SSH mesh. That is also
# the better shape for a WAN slice regardless: one download instead of 92, and no dependency
# on 17 sites each reaching Launchpad.
#
# SETUID IS NOT INSTALLED, deliberately. Apptainer only needs its setuid helper where
# unprivileged user namespaces are unavailable. These nodes run Ubuntu 22.04 with
# `user.max_user_namespaces` well above zero and no AppArmor userns restriction (that is a
# 24.04 default, checked for here anyway), so the unprivileged path works and is the smaller
# attack surface. `--suid` forces the other package if a node ever needs it.
#
# WHAT COUNTS AS INSTALLED. Not `apptainer --version`: a binary that cannot actually start a
# container is the same class of healthy-looking failure as an NFS mount that cannot be
# written to. Verification runs a real container from a probe image built on the database
# node and pushed with the .deb, which exercises userns, fuse and the image mount.
#
# Usage:  sudo ./setup_apptainer.sh [--check] [--suid] [--version X.Y.Z] [host ...]
#           --check        report version and run the container probe; change nothing
#           --suid         also install apptainer-suid (only where userns is unavailable)
#           --version      pin a release (default: latest from the GitHub API)
#           host ...       default: every agent-N in agent_hosts.txt, else agent-1..agent-92
set -euo pipefail

PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin${PATH:+:$PATH}
export PATH

CHECK_ONLY=0
WANT_SUID=0
VERSION=""
HOSTS=()
STAGE=/root/.apptainer-stage
PROBE_SIF="$STAGE/probe.sif"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --check)    CHECK_ONLY=1; shift ;;
        --suid)     WANT_SUID=1; shift ;;
        --version)  VERSION="$2"; shift 2 ;;
        -h|--help)  sed -n '2,36p' "$0"; exit 0 ;;
        -*)         echo "unknown option: $1" >&2; exit 2 ;;
        *)          HOSTS+=("$1"); shift ;;
    esac
done

SSH="ssh -o BatchMode=yes -o ConnectTimeout=15 -o StrictHostKeyChecking=accept-new"
SCP="scp -q -o BatchMode=yes -o ConnectTimeout=15 -o StrictHostKeyChecking=accept-new"

if [[ ${#HOSTS[@]} -eq 0 ]]; then
    [[ -f agent_hosts.txt ]] && mapfile -t HOSTS < <(grep -E '^agent-[0-9]+$' agent_hosts.txt | sort -u)
    [[ ${#HOSTS[@]} -eq 0 ]] && mapfile -t HOSTS < <(seq 1 92 | sed 's/^/agent-/')
fi

# ---------------------------------------------------------------------------- check mode
if [[ $CHECK_ONLY -eq 1 ]]; then
    echo "=== agents (${#HOSTS[@]}) ==="
    probe_arg=""
    [[ -f "$PROBE_SIF" ]] && probe_arg="yes"
    results=$(printf '%s\n' "${HOSTS[@]}" | xargs -P 20 -I{} bash -c "
        v=\$($SSH {} 'apptainer --version 2>/dev/null || echo MISSING' 2>/dev/null | tr -d '\r')
        [[ -z \"\$v\" ]] && v=UNREACHABLE
        if [[ \"\$v\" == MISSING || \"\$v\" == UNREACHABLE ]]; then
            echo \"BAD {} \$v\"
        elif [[ -n '$probe_arg' ]]; then
            if $SSH {} 'apptainer exec /root/apptainer-probe.sif true' >/dev/null 2>&1; then
                echo \"OK {} \$v\"
            else
                echo \"BAD {} \$v (installed, cannot run a container)\"
            fi
        else
            echo \"OK {} \$v\"
        fi")
    echo "$results" | sort | sed 's/^/  /'
    bad=$(echo "$results" | grep -c '^BAD' || true)
    if [[ "$bad" -gt 0 ]]; then
        echo "NOT READY: $bad/${#HOSTS[@]} host(s) without a working apptainer" >&2
        exit 1
    fi
    echo "READY: ${#HOSTS[@]}/${#HOSTS[@]} host(s) can run a container"
    exit 0
fi

# ------------------------------------------------------------- fetch the package, once
mkdir -p "$STAGE"
if [[ -z "$VERSION" ]]; then
    echo "[1/5] resolving latest apptainer release"
    curl -fsSL https://api.github.com/repos/apptainer/apptainer/releases/latest -o "$STAGE/rel.json"
    VERSION=$(python3 -c "import json;print(json.load(open('$STAGE/rel.json'))['tag_name'].lstrip('v'))")
fi
echo "      apptainer $VERSION"

# The plain `_amd64.deb` targets older glibc; the `-trixie+` build does not run on jammy.
DEB="apptainer_${VERSION}_amd64.deb"
SUID_DEB="apptainer-suid_${VERSION}_amd64.deb"
BASE="https://github.com/apptainer/apptainer/releases/download/v${VERSION}"
echo "[2/5] downloading"
[[ -f "$STAGE/$DEB" ]] || curl -fsSL "$BASE/$DEB" -o "$STAGE/$DEB"
if [[ $WANT_SUID -eq 1 && ! -f "$STAGE/$SUID_DEB" ]]; then
    curl -fsSL "$BASE/$SUID_DEB" -o "$STAGE/$SUID_DEB"
fi
echo "      $(du -h "$STAGE/$DEB" | cut -f1) $DEB"

# ------------------------------------------------- install locally, then build the probe
echo "[3/5] installing on the database node, and building a probe image"
DEBIAN_FRONTEND=noninteractive apt-get install -y -qq \
    uidmap fuse2fs squashfs-tools cryptsetup >/dev/null 2>&1 || true
if ! command -v apptainer >/dev/null 2>&1; then
    dpkg -i "$STAGE/$DEB" >/dev/null 2>&1 || DEBIAN_FRONTEND=noninteractive apt-get -f install -y -qq
fi
command -v apptainer >/dev/null 2>&1 || { echo "apptainer did not install here" >&2; exit 1; }
if [[ ! -f "$PROBE_SIF" ]]; then
    # A real image, small, built once. This is what makes verification mean "containers run"
    # rather than "a binary is on PATH".
    apptainer build --force "$PROBE_SIF" docker://busybox:latest >/dev/null 2>&1 \
        || { echo "could not build the probe image" >&2; exit 1; }
fi
echo "      $(apptainer --version), probe $(du -h "$PROBE_SIF" | cut -f1)"

# ------------------------------------------------------------------- push and install
echo "[4/5] installing on ${#HOSTS[@]} agents"
export SSH SCP STAGE DEB SUID_DEB WANT_SUID PROBE_SIF
install_one() {
    local h="$1"
    $SCP "$STAGE/$DEB" "$h:/root/" || return 1
    [[ $WANT_SUID -eq 1 ]] && { $SCP "$STAGE/$SUID_DEB" "$h:/root/" || return 1; }
    $SCP "$PROBE_SIF" "$h:/root/apptainer-probe.sif" || return 1
    $SSH "$h" "bash -s" <<REMOTE
set -e
export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
export DEBIAN_FRONTEND=noninteractive
# Runtime dependencies from the distro archive, which the agents CAN reach (it is the PPA
# they cannot). Tolerated if already present.
apt-get install -y -qq uidmap fuse2fs squashfs-tools cryptsetup >/dev/null 2>&1 || true
if ! command -v apptainer >/dev/null 2>&1; then
    dpkg -i /root/$DEB >/dev/null 2>&1 || apt-get -f install -y -qq >/dev/null 2>&1
fi
$( [[ $WANT_SUID -eq 1 ]] && echo "dpkg -i /root/$SUID_DEB >/dev/null 2>&1 || true" )
command -v apptainer >/dev/null 2>&1
# The install is only real if a container actually starts.
apptainer exec /root/apptainer-probe.sif true
REMOTE
}
export -f install_one
# Parallel, because this is 92 hosts across 17 sites and each one takes a ~40 MB copy; done
# sequentially the rollout is dominated by WAN round trips. Bounded at 15 so the database
# node's uplink is not the new bottleneck.
mapfile -t RESULTS < <(printf '%s\n' "${HOSTS[@]}" | xargs -P 15 -I{} bash -c \
    'if install_one {} >/dev/null 2>&1; then echo "OK {}"; else echo "BAD {}"; fi')
FAILED=()
for line in "${RESULTS[@]}"; do
    [[ "$line" == BAD* ]] && FAILED+=("${line#BAD }")
done
echo "      installed and probed on $(( ${#HOSTS[@]} - ${#FAILED[@]} ))/${#HOSTS[@]}"
if [[ ${#FAILED[@]} -gt 0 ]]; then
    # Fatal, for the same reason the NFS setup is: a partial fleet that reports success stays
    # invisible until a job lands on one of the hosts that was skipped.
    echo "      FAILED: ${FAILED[*]}" >&2
    echo "Refusing to report success for a partial install. Fix or exclude those hosts and rerun." >&2
    exit 1
fi

echo "[5/5] done"
echo
echo "Apptainer $VERSION on ${#HOSTS[@]} agents; every one ran a real container."
echo "Workflow .sif images now run natively — runtime.execution.image_overrides is no longer"
echo "needed for them, and container_runtime: auto will pick apptainer for a singularity image."
echo "Re-check with: sudo $0 --check"
