#!/bin/bash

VERSION=0.13.1
URL=http://repo.scala-sbt.org/scalasbt/sbt-native-packages/org/scala-sbt/sbt/$VERSION/sbt.zip

function usage {
	echo ""
	echo "    Usage: $0 [-f]"
	echo ""
	echo "    Attempts to retrieve SBT $VERSION from $URL."
	echo "    If sbt.dir already exists, this will be skipped, unless the -f flag is specified."
	echo ""
	exit 1
}

cd `dirname $0`

[ "$1" != "-f" -a -d sbt.dir ] && usage

if hash wget 2>/dev/null; then
 wget -O /tmp/sbt.zip $URL
elif hash curl 2>/dev/null; then
 curl -o /tmp/sbt.zip $URL
else
 echo "You need curl or wget installed to download sbt."
 usage
fi

rm -fr sbt ; unzip /tmp/sbt.zip ; mv sbt sbt.dir
ln -s sbt.dir/bin/sbt sbt
