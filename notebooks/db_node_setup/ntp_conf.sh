#!/usr/bin/env bash
# Chrony configuration for the slice, in one place.
#
# Sourced by BOTH ways of applying it: 07_ntp.sh (the slice-build pipeline, which reaches
# nodes over the bootstrap/bastion transport in lib.sh) and fix_slice_clocks.sh (the
# operational tool, which uses the root SSH mesh on a slice that is already up). One
# definition, because the two transports exist for different situations but the fleet must
# end up with the same clock configuration either way.

# Where the two drop-in files live. conf.d is included by the stock Ubuntu chrony.conf, so
# nothing in the distribution's file is edited.
NTP_DB_CONF=/etc/chrony/conf.d/swarm-slice.conf
NTP_AGENT_CONF=/etc/chrony/conf.d/swarm-agent.conf

# The database node serves time to the slice.
read -r -d '' NTP_DB_BODY <<'EOF' || true
# Serve time to the slice (SWARM ntp_conf.sh).
allow 10.0.0.0/8
allow 192.168.0.0/16
# Keep serving from the local clock if upstream NTP becomes unreachable. An internally
# consistent fleet is what the measurements depend on, and it is strictly better than nodes
# free-running apart from each other; drifting together from UTC costs nothing here.
local stratum 10
EOF

# Every agent takes time from it.
read -r -d '' NTP_AGENT_BODY <<'EOF' || true
# Take time from the slice database node (SWARM ntp_conf.sh). The public pools this image
# ships with are unreachable from most sites on this testbed.
server database iburst prefer
# Step rather than slew whenever the error exceeds 0.1s, at ANY time -- chrony's default
# ("makestep 1 3") steps only during the first three updates and slews afterwards. Nodes
# here are found ~0.5s out mid-campaign, and slewing that back at the default rate takes
# hours, during which every cross-node measurement is quietly wrong.
makestep 0.1 -1
EOF
