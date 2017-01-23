# Cloud-Computing
Cloud Computing Semester Project

# Cloning Script
This folder contains the shell script to clone Cassandra, Hbase and Redis VMs.
  - This script takes care of shutting down the node cleanly and making a clone of it in the desired remote machine.
  - IP address changes and config file changes are also handled by the scirpt.
  - The script reboots both the VMs and the new node automatically joins the cluster.
  - partition.sh takes care of mounting an external qcow2 disk for the new VM and partitioning it. This is done to prevent the   VM from growning in size.
  

# RedisDHT
- This folder contains the implementation of Redis functionality like adding a new node, removing a node, assigning hash slots, load balancing of hash slots, etc.

- This folder also contains the launcher scripts to start redis nodes in different machine.

# Redis YCSB Change
YCSB by default does not support Redis in cluster mode. Code changes have been done to make YCSB to run in cluster mode.
