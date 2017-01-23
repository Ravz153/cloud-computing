#!/bin/bash

# need to write script for cleaning up cassandra data/hbase data/redis data from the newly launched VM.
source $1

if [ "$database" = "cassandra" ]
then
	echo "Cassandra"
	echo "Stopping Cassandra in ${old_ip_value}"
	ssh "root@${old_ip_value}" "/root/dsc-cassandra-3.0.9/bin/stop-server"

elif [ "$database" = "hbase" ]
then
	echo "hbase"
	#echo "Stopping Hbase in ${old_ip_value}"

elif [ "$database" = "redis" ]
then
	echo "Redis"
	# echo "Stopping Redis in ${old_ip_value}"

fi

# shutdown the VM you are cloning
# echo "Shuting Down ${original_vm}"
# virsh shutdown $original_vm

# copy the orignal VM you want to clone
echo "Copying ${original_vm} to ${new_vm}"
cp $original_vm.qcow2 $new_vm.qcow2

# mount the new VMs disk to host machine to edit network files
echo "Mounting ${new_vm}.qcow2 to /mnt/vm_disk/"
guestmount -a $new_vm.qcow2 -m /dev/centos_a2/root /mnt/vm_disk/

echo "Changing ip address to ${new_ip_value}"
#sed -ie 's/'$local_ip_value'/'$new_ip_value'/' /mnt/vm_disk/etc/sysconfig/network-scripts/ifcfg-eth0
sed -i 's#IPADDR=.*#IPADDR=\x22'${new_ip_value}'\x22#' /mnt/vm_disk/etc/sysconfig/network-scripts/ifcfg-eth0

#change hostname
echo $new_hostname | cat > /mnt/vm_disk/etc/hostname

#remove from /etc/fstab
sed -i '/vgpool-lvdata/d' /mnt/vm_disk/etc/fstab

# copy partition script into /root of VM...Will be invoked when VM boots up
cp ./config.ini /mnt/vm_disk/root
cp ./partition.sh /mnt/vm_disk/root
chmod +x /mnt/vm_disk/root/partition.sh

if [ "$database" = "cassandra" ]
then
	echo "modifying the yaml file"
	sed -ie 's/'$old_ip_value'/'$new_ip_value'/' /mnt/vm_disk/root/dsc-cassandra-3.0.9/conf/cassandra.yaml
	echo "Changing external disk location in yaml"
	sed -ie 's/'$old_hostname'/'$new_hostname'/' /mnt/vm_disk/root/dsc-cassandra-3.0.9/conf/cassandra.yaml
elif [ "$database" = "hbase" ]
then
	echo "modifying the hbase files"
	#TODO
	#copy regionsserves file, slaves file and /etc/hosts file from an actively running hbase node.
	#for now modifying manually. Since not much work.
	#also modify the name node and hbase master to know about the new node.

elif [ "$database" = "redis" ]
then
	echo "modifying the redis files"

fi

#unmount the disk after all changes have been made
echo "Unmounting ${new_vm}.qcow2"
guestunmount /mnt/vm_disk/

echo "Copying ${original_vm}.xml to ${new_vm}.xml"
# cd /etc/libvirt/qemu
cp "${original_vm}.xml" "${new_vm}.xml"

echo "Recplacing uuid"
sed -i 's#<uuid>.*#<uuid>'$uuid'</uuid>#' "${new_vm}.xml"

echo "Recplacing mac address"
sed -i 's#<mac address=.*>#<mac address=\x27'$mac'\x27/>#' "${new_vm}.xml"

echo "replace old_domain with new_domain in the xml file"
sed -i -- 's/'${original_vm}'/'${new_vm}'/g' "${new_vm}.xml"

replace_str=""
replace_with_str=""
if [ "$database" = "cassandra" ]
then
	original_hostname=$(echo ${original_vm} | cut -c11-)
	replace_str=$original_hostname
	replace_with_str=$new_hostname
elif [ "$database" = "hbase" ]
then
	replace_str=$hbase_old_datadir
	replace_with_str=$hbase_new_datadir
elif [ "$database" = "redis" ]
then
	replace_str=$original_hostname
        replace_with_str=$new_hostname
fi

sed -i -- 's/'${replace_str}'/'${replace_with_str}'/g' "${new_vm}.xml"


# cd /var/lib/libvirt/images
echo "Creating 128GB external disk"
qemu-img create -f qcow2 "${replace_with_str}_data.img" 128G

# copy image and xml and external harddisk to target machine
echo "Copying external harddisk"
scp "${replace_with_str}_data.img" "root@${target}:/var/lib/libvirt/images"


echo "Copying main disk"
scp "${new_vm}.qcow2" "root@${target}:/var/lib/libvirt/images"

echo "Copying VM xml"
scp "${new_vm}.xml" "root@${target}:/etc/libvirt/qemu"

echo "Cleaning up local machine"
rm -f "${replace_with_str}_data.qcow2"
rm -f "${new_vm}.qcow2"
rm -f "${new_vm}.xml"


# gparted version causing issue...Remove it.
# some are raw and some are qcow2. Stick to qcow2

# remotely execute virsh define and start
echo "Remotely starting ${new_vm} at ${target}"
ssh "root@${target}" "virsh define /etc/libvirt/qemu/${new_vm}.xml; virsh create /etc/libvirt/qemu/${new_vm}.xml"


# echo "Starting original VM Back"
virsh start ${original_vm}

# waiting for VM to boot up
echo "Sleeping for 20 seconds"
sleep 20

# going to execute the partition script to partition the external disk
ssh "root@${new_ip_value}" "~/partition.sh ~/config.ini"

if false
then

	if [ "$database" = "cassandra" ]
	then
		echo "Starting cassandra"
		ssh "root@${new_ip_value}" "/root/dsc-cassandra-3.0.9/bin/cassandra"
	elif [ "$database" = "hbase" ]
	then
		echo "modifying the hbase files"

	elif [ "$database" = "redis" ]
	then
		echo "modifying the redis files"
	fi

fi
