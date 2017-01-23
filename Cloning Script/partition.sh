#!/bin/sh

source $1

echo "Creating partition vdb1"
hdd="/dev/vdb"
for i in $hdd;do
echo "n
p
1


t
8e
w
"|fdisk $i;done

echo "Creating a Physical volume on /dev/vdb1"
pvcreate /dev/vdb1

echo "Creating a Volume Group on  /dev/vdb1"
vgcreate vgpool /dev/vdb1

echo "Extenting the logical volume to occupy 100% Free space"
lvcreate -n lvdata -l 100%FREE vgpool

echo "Formatting Partition to ext3"
mkfs -t ext3 /dev/mapper/vgpool-lvdata

# Create a mount point
rm -rf /mnt/*

replace_with_str=""
if [ "$database" = "cassandra" ]
then
	replace_with_str=$new_hostname
elif [ "$database" = "hbase" ]
then
	replace_with_str=$hbase_new_datadir
fi

mkdir "/mnt/${replace_with_str}_data"

# mount the partition to the mount point
mount -t ext3 /dev/mapper/vgpool-lvdata "/mnt/${replace_with_str}_data/"

if [ "$database" = "cassandra" ]
then
	echo "Creating commitlogs, hints, data, saved_cahces"
	mkdir -p "/mnt/${new_hostname}_data/data/commitlog"
	mkdir -p "/mnt/${new_hostname}_data/data/saved_caches"
	mkdir -p "/mnt/${new_hostname}_data/data/hints"
	mkdir -p "/mnt/${new_hostname}_data/data/data"

elif [ "$database" = "hbase" ]
then
	# create softlink
	echo "Creating softlink to external hard disk"
	mkdir -p "/mnt/${new_hostname}_data/hadoop_store/datanode"
	ln -s "/mnt/${new_hostname}_data/hadoop_store/datanode" /home/cloudvm1/hadoop_store/datanode

elif [ "$database" = "redis" ]
then
	echo "Redis Case"
fi

# add entry to fstab file
fstab_entry='/dev/mapper/vgpool-lvdata  /mnt/*_data   ext3     defaults        0 1'
fstab_entry="${fstab_entry/\*/$replace_with_str}"
echo $fstab_entry >> /etc/fstab
