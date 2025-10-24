#!/bin/bash

# Set vm.swappiness to 1
echo "Setting vm.swappiness to 1..."

# Set the current swappiness value
sudo sysctl -w vm.swappiness=1

# Make it persistent across reboots
if ! grep -q "^vm.swappiness" /etc/sysctl.conf 2>/dev/null; then
    echo "vm.swappiness=1" | sudo tee -a /etc/sysctl.conf > /dev/null
    echo "Added vm.swappiness=1 to /etc/sysctl.conf"
else
    sudo sed -i 's/^vm.swappiness=.*/vm.swappiness=1/' /etc/sysctl.conf
    echo "Updated vm.swappiness in /etc/sysctl.conf"
fi

# Verify the setting
current_value=$(cat /proc/sys/vm/swappiness)
echo "Current vm.swappiness value: $current_value"
