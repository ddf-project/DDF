#!/bin/bash

export PA_HOME="$(cd `dirname ${BASH_SOURCE[0]}`/../ >/dev/null 2>&1; echo $PWD)"
 export DDF_HOME=${PA_HOME}/../

 ${DDF_HOME}/pa/exe/build_and_deploy_jars.sh
 ${DDF_HOME}/pa/exe/start-pa-server.sh --cluster yarn
