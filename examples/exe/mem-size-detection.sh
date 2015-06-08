#!/bin/bash

system=`uname -s`

if [ "X$system" == "XDarwin" ]; then
	# on Mac
	free_mem=`top -l 1 | head -n 10 | grep PhysMem | cut -d',' -f 2 | cut -d' ' -f2 | cut -d'M' -f1`
elif [ "X$system" == "XLinux" ]; then
	# on Linux
	free_mem=`free -m | sed -n 2p | tr -s ' ' | cut -d' ' -f4`
else
	echo "Only MacOS and Linux are supported"; exit 1
fi

if [ "$free_mem" -le "1024" ]; then
	export SPARK_MEM=512m
else
	export SPARK_MEM=`expr $free_mem - 512`m
fi

echo
echo "###########################"
echo "# Calculating Memory Size #"
echo "###########################"
echo
echo -e "\t SPARK_MEM="$SPARK_MEM
