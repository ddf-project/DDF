#!/bin/bash
set -ex

version=$1

if [[ -z $version ]]; then
  echo 'Usage: bump-version.sh <version>'
  exit 1
fi

SCRIPT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
SOURCE_ROOT=$SCRIPT_DIR/..

$SOURCE_ROOT/bin/sbt clean

sed -e "s/val rootVersion = \".*\"/val rootVersion = \"$version\"/g" \
    -i '' $SOURCE_ROOT/project/RootBuild.scala

$SOURCE_ROOT/bin/make-poms.sh

echo "Last thing: please edit pom.xml to change to <version>$version</version>"