#!/bin/sh

for h in `cat /root/spark-ec2/slaves `; do 
	echo $h 
	ssh $h 'rm -rf /mnt{,1,2,3}/spark/*' 
	ssh $h 'rm -rf /mnt{,1,2,3}/tmp/*'
	ssh $h 'rm -rf /tmp/*' 
done