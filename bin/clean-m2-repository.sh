#!/bin/bash
#
# This scans your ~/.m2 local repository and removes all .pom for which there
# is no corresponding .jar. This situation can cause sbt compile errors.
#
POMS=(`find ~/.m2 -name *.pom`)
for pom in ${POMS[*]} ; do
  base="`dirname $pom`/`basename $pom .pom`"
  jar="`dirname $pom`/`basename $pom .pom`".jar
  if [ ! -f "$base".jar ] ; then
    echo rm -f "$base".*
    rm -f "$base".*
  else
    echo -n "."
  fi
done

echo ""
