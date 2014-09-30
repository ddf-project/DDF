#!/bin/bash
#
# Load project/RootBuild.scala and generates pom.xml for all the sub-projects.
# This should NOT normally be run by developers. Rather it is to be run once in a while
# by maintainers, and the resulting pom.xml checked into the repo for mvn use by developers.
#

BINDIR="`dirname $0`"
cd "$BINDIR"/..

SBT="bin/sbt"
$SBT make-pom

TARGET_DIR=target
# Project list is heuristically determined to be those dirs that have src/main/ under them
PROJECTS=(`echo */src/main | sed -e 's/\/src\/main//g'`)

for project in ${PROJECTS[*]} ; do
  # find the latest *.pom under $TARGET_DIR
  pom=`find $project -type f -name *.pom -exec ls -l {} \; 2> /dev/null | sort -t' ' -k +6,6 -k +7,7 -k+8,8 | tail -1 | sed -e 's/.* //g'`
  [ -z $pom ] && continue
  echo "Copying latest $pom to $project/pom.xml"
  # Notice there's a hack to pass the "*" exclusion wildcard from sbt to pom.xml.
  # This is because sbt refuses to generate "*" for pom.xml, citing that it's not supported/legal.
  # But in fact, mvn supports it, even though it's "bad practice". Here, we do it because
  # we need it to get out of the Shark jar hell.
  sed -e 's/_MAKE_POM_EXCLUDE_ALL_/\*/g' $pom > $project/pom.xml
done
