#!/bin/sh

usage() {
	echo "Usage: deploydist.sh bigr_version"
	exit 1
}

[[ -z "$1" ]] && usage ; version=$1
local_repos=/tmp/distrepos

DIR="$(cd `dirname $0`/../ 2>&1 >/dev/null; echo $PWD)"
mvn deploy:deploy-file -Durl=file://${local_repos} -DrepositoryId=bigr -Dfile=${DIR}/target/scala-2.9.3/bigr_server_2.9.3-${version}-bin.tar.gz -DgroupId=adatao.bigr -DartifactId=bigr_server_2.9.3 -Dversion=${version} -Dpackaging=tar.gz
#mvn deploy:deploy-file -Durl=file://${local_repos} -DrepositoryId=bigr -Dfile=${DIR}/target/scala-2.9.3/bigr_server_2.9.3-${version}-bin.zip -DgroupId=adatao.bigr -DartifactId=bigr_server_2.9.3 -Dversion=${version} -Dpackaging=zip

rsync -avz --delete /tmp/distrepos root@canary.adatao.com:/root/spark-admin/static/