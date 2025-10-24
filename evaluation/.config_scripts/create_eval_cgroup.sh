#!/bin/bash

# cgroup v2 mount point
CGROUP_ROOT="/sys/fs/cgroup"

# Create two cgroup groups
BASELINE_GROUP="vista_base"
NOHP_GROUP="vista_nohp"

# Get NUMA node CPU ranges
NUMA0_CPUS=$(cat /sys/devices/system/node/node0/cpulist)
NUMA1_CPUS=$(cat /sys/devices/system/node/node1/cpulist)

# Parse NUMA node ranges
NUMA0_START=$(echo $NUMA0_CPUS | awk -F'-' '{print $1}')
NUMA0_END=$(echo $NUMA0_CPUS | awk -F'-' '{print $2}')
NUMA1_START=$(echo $NUMA1_CPUS | awk -F'[-,]' '{print $1}')
NUMA1_END=$(echo $NUMA1_CPUS | awk -F'[-,]' '{if ($2 != "") print $2; else print $1}')

# vista_base: 32 CPUs from NUMA0
BASELINE_CPUS="$NUMA0_START-$((NUMA0_START + 31))"

# vista_nohp: 32 CPUs (16 from NUMA0, 16 from NUMA1)
NUMA0_16CPUS="$NUMA0_START-$((NUMA0_START + 15))"
NUMA1_16CPUS="$NUMA1_START-$((NUMA1_START + 15))"
NOHP_CPUS_LIST="$NUMA0_16CPUS,$NUMA1_16CPUS"

# Function to remove cgroup safely
remove_cgroup() {
    local group=$1
    if [ -d "$CGROUP_ROOT/$group" ]; then
        # Kill all processes in the cgroup first
        if [ -f "$CGROUP_ROOT/$group/cgroup.procs" ]; then
            for pid in $(cat "$CGROUP_ROOT/$group/cgroup.procs"); do
                echo "Moving process $pid to root cgroup"
                sudo sh -c "echo $pid > $CGROUP_ROOT/cgroup.procs" 2>/dev/null || true
            done
        fi
        # Remove the cgroup directory
        sudo rmdir "$CGROUP_ROOT/$group" 2>/dev/null && echo "Removed existing $group" || echo "Failed to remove $group"
    fi
}

# Remove existing cgroups
remove_cgroup "$BASELINE_GROUP"
remove_cgroup "$NOHP_GROUP"

# Enable controllers at root level
sudo sh -c "/usr/bin/echo '+cpu +cpuset +memory' > $CGROUP_ROOT/cgroup.subtree_control"

# Create vista_base group (32 CPUs from NUMA0, 64GB)
sudo mkdir -p "$CGROUP_ROOT/$BASELINE_GROUP"
echo "Created $BASELINE_GROUP"

# Set limits for vista_base - 32 CPUs from NUMA0, NUMA0 memory, 64GB
sudo sh -c "/usr/bin/echo '$BASELINE_CPUS' > $CGROUP_ROOT/$BASELINE_GROUP/cpuset.cpus"
sudo sh -c "/usr/bin/echo '0' > $CGROUP_ROOT/$BASELINE_GROUP/cpuset.mems"
sudo sh -c "/usr/bin/echo '64G' > $CGROUP_ROOT/$BASELINE_GROUP/memory.max"
echo "Set $BASELINE_GROUP: CPUs=$BASELINE_CPUS (32 CPUs from NUMA0), Memory node=0, Limit=64G"

# Create vista_nohp group (32 CPUs evenly distributed, 60GB)
sudo mkdir -p "$CGROUP_ROOT/$NOHP_GROUP"
echo "Created $NOHP_GROUP"

# Set limits for vista_nohp - 32 CPUs (16 from NUMA0, 16 from NUMA1), both memory nodes, 60GB
sudo sh -c "/usr/bin/echo '$NOHP_CPUS_LIST' > $CGROUP_ROOT/$NOHP_GROUP/cpuset.cpus"
sudo sh -c "/usr/bin/echo '0-1' > $CGROUP_ROOT/$NOHP_GROUP/cpuset.mems"
sudo sh -c "/usr/bin/echo '60G' > $CGROUP_ROOT/$NOHP_GROUP/memory.max"
echo "Set $NOHP_GROUP: CPUs=$NOHP_CPUS_LIST (32 CPUs: 16 from NUMA0, 16 from NUMA1), Memory nodes=0-1, Limit=60G"

# Give ownership to current user
USER=$(whoami)
sudo chown -R $USER:$USER "$CGROUP_ROOT/$BASELINE_GROUP"
sudo chown -R $USER:$USER "$CGROUP_ROOT/$NOHP_GROUP"

sudo chmod o+w /sys/fs/cgroup/cgroup.procs
sudo chmod o+w /sys/fs/cgroup/$BASELINE_GROUP/cgroup.procs
sudo chmod o+w /sys/fs/cgroup/$NOHP_GROUP/cgroup.procs

echo "cgroup v2 groups created successfully"

