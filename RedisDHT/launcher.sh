#!/bin/bash

ip=()
port=()
node_count=0
current_line=1
while read line
do
	if [ $current_line -eq 1 ]; then
		node_count=$(echo $line)
		let node_count=$node_count+0
	else
		node_id=$(echo $line | cut -f1 -d ":")
		ip_addr=$(echo $line | cut -f2 -d ":")
		ip[$node_id]=$ip_addr
		port_addr=$(echo $line | cut -f3 -d ":")
		port[$node_id]="$port_addr"
	fi
	
	let current_line+=1

done < $1

for node_id in $(seq 1 $(expr $node_count))
do
	temp_ip=${ip[$node_id]}
	temp_port=${port[$node_id]}

	ssh -o StrictHostKeyChecking=no root@$temp_ip "java -cp '/root/RedisDHT/bin/' MainDriver ${temp_ip}':'${temp_port}" &
done


