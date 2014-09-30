#!/bin/bash

echo
echo "#####################"
echo "# Installing Rserve #"
echo "#####################"
echo
R -q -e "install.packages('Rserve',lib='/mnt/BigR-master/server/rlibs/',contriburl=contrib.url('http://cran.cnr.Berkeley.edu/'))"