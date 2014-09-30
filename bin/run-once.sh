#!/bin/bash
#
# Run once to set up a new dev environment
#

ME=`basename $0 .sh`
DIR="`dirname $0`"
cd "$DIR/.."

SBT=bin/sbt
LOGFILE="$DIR/$ME.log"
touch $LOGFILE

function Run() {
  echo "
  ---------------------------------------------------------------------------------------


  ***************************************************************************************
  $ME.sh run by `whoami`@`hostname` at `date` in `pwd`
  ***************************************************************************************
  "


  echo "
  ***************************************************************************************
  Retrieving sbt
  ***************************************************************************************
  "
	bin/get-sbt.sh


  # Don't do this automatically; it seems too expensive in terms of people having to re-download stuff
  #echo "
  #***************************************************************************************
  #Cleaning up ~/.m2 to help sbt avoid missing jar issues
  #***************************************************************************************
  #"
  #bin/clean-m2-repository.sh


  echo "
  #***************************************************************************************
  #Re-installing the unmanaged libs in ~/.m2 and ~/.ivy2
  #***************************************************************************************
  #"
  #rm -fr ~/.m2/repository/{adatao,org/spark-project,edu/berkeley}
  #rm -fr ~/.ivy2/{cache,local}/{adatao.*,org.spark-project,edu.berkeley.*}
  spark/lib/mvn-install-jars.sh || Error "mvn-install-jars.sh failed"


  echo "
  ***************************************************************************************
  Running one time 'sbt clean package eclipse' to generate .classpath and .project files
  ***************************************************************************************
  "
  #$SBT clean compile package eclipse || Error "$SBT clean compile package eclipse failed"
  # Don't exit on failure; we may still be able to get mvn going
  $SBT clean compile package eclipse


  echo "
  ***************************************************************************************
  Running one time 'bin/make-poms.sh' to generate sub-project pom.xml files
  ***************************************************************************************
  "
  bin/make-poms.sh || Error "make-poms.sh failed"


  echo "
  ***************************************************************************************
  Running one time 'mvn clean install -DskipTests' to confirm successful build
  ***************************************************************************************
  "
  mvn clean install -DskipTests || exit 1
  
  echo "
  ***************************************************************************************
  Done. You can now continue to develop using sbt or mvn
  ***************************************************************************************
  "
}

function Error() {
	local MSG=$1
  echo "
  ***************************************************************************************
  Error: ${MSG:=Build error}

  NB: If you see something like 'Invalid target release', be sure you have JAVA_HOME set
  ***************************************************************************************
	"
	exit 1
}

#Run 2>&1 | tee -a $LOGFILE
Run
