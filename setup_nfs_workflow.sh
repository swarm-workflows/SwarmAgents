#!/usr/bin/env bash
# Give the slice one shared directory, so a workflow's code, container image and data files
# resolve to the same path on every agent. Run from the database node as root.
#
# WHY THIS EXISTS. `Job.execute()` sleeps for the job's wall time; making it actually run a
# workflow's executables needs the inputs to be *somewhere the agent can read*, and the fleet
# has no shared filesystem -- `execute()`'s two staging TODOs are still TODOs. NFS takes that
# problem off the critical path: with one export mounted at an identical path everywhere, a
# job record's `pfn` and its `data_in` names resolve on whichever agent wins it, and real
# execution can be built and debugged without first building distributed staging.
#
# WHAT IT COSTS, AND IT IS NOT SMALL. A shared filesystem is not what the real system has,
# and this changes what a run measures:
#
#   * **No transfer time is being measured.** Reads come from NFS, not from an agent-to-agent
#     transfer, so `transfer_in_time`/`transfer_out_time` and anything derived from
#     `data_in`/`data_out` sizes describe the export, not the system. Do not report a data
#     movement number from an NFS run.
#   * **Placement stops costing what it should.** Part of what the scheduler is *for* is
#     putting a job near its data. Over one export every agent is equidistant, so the
#     connectivity and DTN penalties price a locality that no longer varies.
#   * **It is a WAN mount.** The agents span 12 subnets across 17 sites; a read from AMST
#     crosses the ocean. Job wall times measured this way include that latency and are not
#     comparable to the Pegasus run they are being compared against, which ran against a
#     local filesystem.
#
# So: NFS is the scaffold that makes execution real, not the configuration any published
# number comes from. Real staging replaces it.
#
# THE CONTAINER IMAGE DOES NOT BELONG HERE. A workflow image is gigabytes (soilmoisture's
# .sif is 3.1 GB); pulling it over a WAN mount on every job start would dominate every
# measurement taken. Stage images to each agent's local disk once -- `--stage-image FILE`
# does exactly that -- and keep the export for code and data.
#
# Usage:  sudo ./setup_nfs_workflow.sh [--check] [--export DIR] [--mount DIR]
#                                      [--stage-image FILE] [host ...]
#           --check           report the state of server, export and every mount; change nothing
#           --export DIR      directory exported by the database node (default /export/swarm-wf)
#           --mount DIR       mount point on the agents (default: the same path as --export)
#           --stage-image F   copy F to <mount-parent>/images on each agent's LOCAL disk
#           host ...          default: every agent-N in agent_hosts.txt, else agent-1..agent-92
set -euo pipefail

# `sudo bash -c` does not give a login shell, so /usr/sbin is off PATH and exportfs,
# systemctl and findmnt are all missing. Set it explicitly rather than relying on how the
# script happened to be invoked.
PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin${PATH:+:$PATH}
export PATH

EXPORT_DIR=/export/swarm-wf
MOUNT_DIR=""
STAGE_IMAGE=""
CHECK_ONLY=0
HOSTS=()

while [[ $# -gt 0 ]]; do
    case "$1" in
        --check)        CHECK_ONLY=1; shift ;;
        --export)       EXPORT_DIR="$2"; shift 2 ;;
        --mount)        MOUNT_DIR="$2"; shift 2 ;;
        --stage-image)  STAGE_IMAGE="$2"; shift 2 ;;
        -h|--help)      sed -n '2,45p' "$0"; exit 0 ;;
        -*)             echo "unknown option: $1" >&2; exit 2 ;;
        *)              HOSTS+=("$1"); shift ;;
    esac
done

# The mount path defaults to the export path. Identical paths on every node is the whole
# point: a job record carries absolute paths (`pfn`, and the working directory a job runs
# in), and those must resolve the same way wherever the job lands. A different mount point on
# the agents would work for exactly as long as nobody read a path out of a job record.
[[ -n "$MOUNT_DIR" ]] || MOUNT_DIR="$EXPORT_DIR"

SSH="ssh -o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=accept-new"

# `dpkg -l <pkg>` exits 0 for a package that is merely *known* to apt -- it prints a "un"
# status line -- so it is not a test for "is installed", and using it as one silently skips
# the install and fails later at the first missing binary. Ask dpkg-query for the status.
pkg_installed() {
    [[ "$(dpkg-query -W -f='${Status}' "$1" 2>/dev/null)" == "install ok installed" ]]
}

if [[ ${#HOSTS[@]} -eq 0 ]]; then
    if [[ -f agent_hosts.txt ]]; then
        mapfile -t HOSTS < <(grep -E '^agent-[0-9]+$' agent_hosts.txt | sort -u)
    fi
    # agent_hosts.txt is deleted by cleanup_between_runs unless it is the resolved
    # --agent-hosts-file, so it is absent as often as not. Fall back to the full slice.
    [[ ${#HOSTS[@]} -eq 0 ]] && mapfile -t HOSTS < <(seq 1 92 | sed 's/^/agent-/')
fi

# ---------------------------------------------------------------------------- check mode
if [[ $CHECK_ONLY -eq 1 ]]; then
    echo "=== database node ==="
    if systemctl is-active --quiet nfs-server 2>/dev/null; then
        echo "nfs-server: active"
    else
        echo "nfs-server: NOT active"
    fi
    echo "export dir: $EXPORT_DIR $([[ -d $EXPORT_DIR ]] && echo present || echo MISSING)"
    echo "exports:"; exportfs -s 2>/dev/null | sed 's/^/  /' || echo "  (none)"
    echo "=== agents (mounted at $MOUNT_DIR) ==="
    results=$(printf '%s\n' "${HOSTS[@]}" | xargs -P 40 -I{} bash -c "
        out=\$($SSH {} \"findmnt -n -o SOURCE --target $MOUNT_DIR 2>/dev/null\" 2>&1) || out=''
        if [[ \"\$out\" == *:* ]]; then echo \"MOUNTED {} \$out\"; else echo \"NOT-MOUNTED {}\"; fi")
    echo "$results" | sort | sed 's/^/  /'
    # Exit non-zero when anything is missing, so a campaign script can gate a run on this
    # instead of parsing the output. A partial setup that reports success is the same class
    # of defect as a mount that exists but cannot be written to: it stays invisible until a
    # job lands on one of the hosts that was never set up.
    unmounted=$(echo "$results" | grep -c '^NOT-MOUNTED' || true)
    if ! systemctl is-active --quiet nfs-server 2>/dev/null || [[ "$unmounted" -gt 0 ]]; then
        echo "NOT READY: $unmounted/${#HOSTS[@]} host(s) unmounted" >&2
        exit 1
    fi
    echo "READY: ${#HOSTS[@]}/${#HOSTS[@]} host(s) mounted"
    exit 0
fi

# ------------------------------------------------------------------------ server, one time
echo "[1/4] NFS server on database -> $EXPORT_DIR"
if ! pkg_installed nfs-kernel-server; then
    DEBIAN_FRONTEND=noninteractive apt-get update -qq
    DEBIAN_FRONTEND=noninteractive apt-get install -y -qq nfs-kernel-server
fi
mkdir -p "$EXPORT_DIR"
# World-writable with the sticky bit: agents run as root but map to an unprivileged uid under
# root_squash, and the workflow's own jobs write outputs here. Sticky stops one job deleting
# another's outputs, which a plain 0777 would allow.
#
# The ownership is not cosmetic and was measured on the slice. A **root-owned** sticky
# directory silently breaks squashed writes: the client can create a file in it (so `touch`
# and `findmnt` both look healthy) but writing content to it fails with EACCES, which is why
# step [3/4] tests a real write rather than the mount. Measured with three directories
# side by side, agent-1 writing over the mount:
#
#   root:root   1777  -> FAIL (create ok, write denied)
#   nobody      1777  -> OK
#   root:root   0777  -> OK
#
# so the sticky bit is only safe on a directory the squashed user owns. Taking that route
# rather than dropping to 0777 keeps jobs from deleting each other's outputs.
chown nobody:nogroup "$EXPORT_DIR"
chmod 1777 "$EXPORT_DIR"

# The slice spans 12 /16s (10.130 .. 10.147), all private and all inside the slice, so one
# 10.0.0.0/8 rule is both simpler and less brittle than enumerating subnets that change
# whenever the slice is rebuilt.
#
# root_squash is kept ON. The agents run as root, and no_root_squash would give every agent
# root-owned write access to the export -- a single bad path in a job record could then
# damage the server. Workflow jobs do not need root to read code or write outputs.
EXPORT_LINE="$EXPORT_DIR 10.0.0.0/8(rw,sync,no_subtree_check,root_squash)"
if ! grep -qF "$EXPORT_DIR " /etc/exports 2>/dev/null; then
    echo "$EXPORT_LINE" >> /etc/exports
else
    # Rewrite in place so a changed option set actually takes effect rather than being
    # appended as a second, shadowed rule for the same directory.
    sed -i "s|^$EXPORT_DIR .*|$EXPORT_LINE|" /etc/exports
fi
exportfs -ra
systemctl enable --now nfs-server >/dev/null 2>&1 || systemctl restart nfs-server
echo "      exported: $(exportfs -s | grep -c "$EXPORT_DIR") rule(s)"

# ------------------------------------------------------------------------------- clients
echo "[2/4] mounting on ${#HOSTS[@]} agents at $MOUNT_DIR"
mount_one() {
    local h="$1"
    $SSH "$h" "bash -s" <<REMOTE
set -e
PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
export PATH
if [[ "\$(dpkg-query -W -f='\${Status}' nfs-common 2>/dev/null)" != "install ok installed" ]]; then
    DEBIAN_FRONTEND=noninteractive apt-get update -qq
    DEBIAN_FRONTEND=noninteractive apt-get install -y -qq nfs-common
fi
mkdir -p "$MOUNT_DIR"
# Idempotent: already mounted is success, not an error to retry past.
if ! findmnt -n --target "$MOUNT_DIR" | grep -q ':'; then
    mount -t nfs -o rw,hard,intr,rsize=1048576,wsize=1048576 database:$EXPORT_DIR "$MOUNT_DIR"
fi
# Survive a reboot. _netdev keeps boot from blocking on an unreachable server, which on a
# WAN slice is the difference between a slow boot and a node that never comes back.
grep -q " $MOUNT_DIR " /etc/fstab || \
    echo "database:$EXPORT_DIR $MOUNT_DIR nfs rw,hard,intr,_netdev 0 0" >> /etc/fstab
REMOTE
}
export -f mount_one 2>/dev/null || true
FAILED=()
for h in "${HOSTS[@]}"; do
    mount_one "$h" >/dev/null 2>&1 || FAILED+=("$h")
done
echo "      mounted on $(( ${#HOSTS[@]} - ${#FAILED[@]} ))/${#HOSTS[@]}"
if [[ ${#FAILED[@]} -gt 0 ]]; then
    # Deliberately fatal rather than a warning. Continuing would run the write probe against
    # the one host that happened to work and print a success banner for a fleet that is only
    # partly set up; the first sign of trouble would then be a job failing on a host nobody
    # remembers skipping.
    echo "      FAILED: ${FAILED[*]}" >&2
    echo "Refusing to report success for a partial setup. Fix or exclude those hosts and rerun." >&2
    exit 1
fi

# ------------------------------------------------------------------- write-through check
# A mount that exists but cannot be written to is the failure this whole step is meant to
# rule out, and it is invisible to `findmnt`. Prove it round-trips before anything depends
# on it: one agent writes, the server reads it back.
echo "[3/4] verifying a write from an agent is visible on the server"
PROBE="$EXPORT_DIR/.nfs-probe-$$"
FIRST="${HOSTS[0]}"
if $SSH "$FIRST" "echo ok > $MOUNT_DIR/.nfs-probe-$$" 2>/dev/null && [[ -f "$PROBE" ]]; then
    echo "      ok ($FIRST wrote, database read)"
    rm -f "$PROBE"
else
    echo "      FAILED: $FIRST could not write a file the server can see" >&2
    rm -f "$PROBE"
    exit 1
fi

# --------------------------------------------------------------- container image staging
echo "[4/4] container image"
if [[ -z "$STAGE_IMAGE" ]]; then
    echo "      skipped (no --stage-image); keep images off the export, see header"
elif [[ ! -f "$STAGE_IMAGE" ]]; then
    echo "      FAILED: $STAGE_IMAGE not found" >&2; exit 1
else
    # Local disk on each agent, deliberately NOT the export -- see the header. Sequential
    # scp of a multi-GB image to 92 WAN hosts is slow; that is a one-time cost, and paying
    # it once beats paying a WAN read on every job start.
    # Sibling of the mount point, so it is on LOCAL disk rather than the export. Printed
    # below because it is derived, not stated: pointing runtime.execution.roots.images at a
    # different directory is a silent misconfiguration that only shows up as every job
    # refusing to find its image.
    IMG_DIR="$(dirname "$MOUNT_DIR")/images"
    BASE="$(basename "$STAGE_IMAGE")"
    SIZE=$(du -h "$STAGE_IMAGE" | cut -f1)
    echo "      staging $BASE ($SIZE) to $IMG_DIR on ${#HOSTS[@]} agents (local disk)"
    for h in "${HOSTS[@]}"; do
        $SSH "$h" "mkdir -p $IMG_DIR" 2>/dev/null || continue
        # Skip a host that already has it at the right size, so a rerun is cheap.
        want=$(stat -c%s "$STAGE_IMAGE")
        have=$($SSH "$h" "stat -c%s $IMG_DIR/$BASE 2>/dev/null || echo 0" 2>/dev/null || echo 0)
        [[ "$have" == "$want" ]] && continue
        scp -q -o BatchMode=yes -o StrictHostKeyChecking=accept-new "$STAGE_IMAGE" "$h:$IMG_DIR/" \
            || echo "      WARN: $h image copy failed" >&2
    done
    echo "      done"
    echo "      set runtime.execution.roots.images: $IMG_DIR"
fi

echo
echo "Export : database:$EXPORT_DIR"
echo "Mount  : $MOUNT_DIR (identical on every node, so job paths resolve everywhere)"
echo "Re-check with: sudo $0 --check"
