#!/bin/bash
#
# @author ctn
# @created Tue Jun 18 14:45:45 PDT 2013
#
# See http://www.mkyong.com/maven/how-to-include-library-manully-into-maven-local-repository/
#

# This works on MacOS, too, where some home dirs have spaces in the name
EXE_DIR=$(cd "$(dirname "$0")" 2>&1 >/dev/null; pwd) ; cd "$EXE_DIR"


function MvnInstall {
  local PACKAGING=jar
  local FILE=${ARTIFACT_ID}-${VERSION}.${PACKAGING}
  local GROUP_ID=`echo $GROUP_ID | sed -e 's/\///g'` # strip any trailing directory slashes
  echo "Installing $GROUP_ID $ARTIFACT_ID $VERSION to local maven repository"
  mvn install:install-file -Dfile=$FILE -DgroupId=$GROUP_ID -DartifactId=$ARTIFACT_ID -Dversion=$VERSION -Dpackaging=jar
}

#GROUP_ID=edu.berkeley.cs.amplab ARTIFACT_ID=shark_2.9.3 VERSION=0.7.0 MvnInstall

function ProcessJar {
  # Example: shark_2.9.3-0.7.0.jar
  local name=`basename $JAR .jar`
  local ARTIFACT_ID=`echo $name | cut -d- -f1`
  local VERSION=`echo $name | cut -d- -f2`
  MvnInstall
}


function ScanDirs {
  local count=`echo *.jar | wc -w | sed 's/ //g'`
  local dir=`basename "$PWD"`
  local i=1

  for jar in *.jar ; do
    echo ""
    echo "$dir/ #$i of $count: $jar"
    i=$((i+1))

# @deprecated
#    # Handle exceptions first
#    if [ "jdo2-api-2.3-ec.jar" == $jar ] ; then
#      # We can't parse this generically because all the hyphens in the name
#      GROUP_ID=javax.jdo ARTIFACT_ID=jdo2-api VERSION=2.3-ec MvnInstall
#      continue
#    fi

    # Now do the normal stuff
    [ -f $jar ] && JAR=$jar ProcessJar
  done

  for dir in */ ; do
    [ -d $dir ] && (cd $dir ; GROUP_ID="$GROUP_ID.$dir" ScanDirs ; cd ..)
  done
}

#GROUP_ID=lib_unmanaged ScanDirs
#GROUP_ID=adatao.unmanaged ScanDirs
GROUP_ID=com.adatao.unmanaged ScanDirs
