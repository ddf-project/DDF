#!/usr/bin/env bash
#

# A library to simplify using the SBT launcher from other packages.
# Note: This should be used by tools like giter8/conscript etc.

# TODO - Should we merge the main SBT script with this library?

if test -z "$HOME"; then
  declare -r script_dir="$(dirname $script_path)"
else
  declare -r script_dir="$HOME/.sbt"
fi

declare -a residual_args
declare -a java_args
declare -a scalac_args
declare -a sbt_commands
declare java_cmd=java
declare -r sbt_bin_dir="$(dirname "$(realpath "$0")")"
declare -r sbt_home="$(dirname "$sbt_bin_dir")"

echoerr () {
  echo 1>&2 "$@"
}
vlog () {
  [[ $verbose || $debug ]] && echoerr "$@"
}
dlog () {
  [[ $debug ]] && echoerr "$@"
}

jar_file () {
  echo "$(cygwinpath "${sbt_home}/bin/sbt-launch.jar")"
}

acquire_sbt_jar () {
  sbt_jar="$(jar_file)"

  if [[ ! -f "$sbt_jar" ]]; then
    echoerr "Could not find launcher jar: $sbt_jar"
    exit 2
  fi
}

execRunner () {
  # print the arguments one to a line, quoting any containing spaces
  [[ $verbose || $debug ]] && echo "# Executing command line:" && {
    for arg; do
      if printf "%s\n" "$arg" | grep -q ' '; then
        printf "\"%s\"\n" "$arg"
      else
        printf "%s\n" "$arg"
      fi
    done
    echo ""
  }

  # THis used to be exec, but we loose the ability to re-hook stty then
  # for cygwin...  Maybe we should flag the feature here...
  "$@"
}

addJava () {
  dlog "[addJava] arg = '$1'"
  java_args=( "${java_args[@]}" "$1" )
}
addSbt () {
  dlog "[addSbt] arg = '$1'"
  sbt_commands=( "${sbt_commands[@]}" "$1" )
}
addResidual () {
  dlog "[residual] arg = '$1'"
  residual_args=( "${residual_args[@]}" "$1" )
}
addDebugger () {
  addJava "-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=$1"
}

# a ham-fisted attempt to move some memory settings in concert
# so they need not be dorked around with individually.
get_mem_opts () {
  local mem=${1:-1024}
  local perm=$(( $mem / 4 ))
  (( $perm > 256 )) || perm=256
  (( $perm < 1024 )) || perm=1024
  local codecache=$(( $perm / 2 ))

  echo "-Xms${mem}m -Xmx${mem}m -XX:MaxPermSize=${perm}m -XX:ReservedCodeCacheSize=${codecache}m"
}

require_arg () {
  local type="$1"
  local opt="$2"
  local arg="$3"
  if [[ -z "$arg" ]] || [[ "${arg:0:1}" == "-" ]]; then
    echo "$opt requires <$type> argument"
    exit 1
  fi
}

is_function_defined() {
  declare -f "$1" > /dev/null
}

process_args () {
  while [[ $# -gt 0 ]]; do
    case "$1" in
       -h|-help) usage; exit 1 ;;
    -v|-verbose) verbose=1 && shift ;;
      -d|-debug) debug=1 && shift ;;

           -ivy) require_arg path "$1" "$2" && addJava "-Dsbt.ivy.home=$2" && shift 2 ;;
           -mem) require_arg integer "$1" "$2" && sbt_mem="$2" && shift 2 ;;
     -jvm-debug) require_arg port "$1" "$2" && addDebugger $2 && shift 2 ;;
         -batch) exec </dev/null && shift ;;

       -sbt-jar) require_arg path "$1" "$2" && sbt_jar="$2" && shift 2 ;;
   -sbt-version) require_arg version "$1" "$2" && sbt_version="$2" && shift 2 ;;
     -java-home) require_arg path "$1" "$2" && java_cmd="$2/bin/java" && shift 2 ;;

            -D*) addJava "$1" && shift ;;
            -J*) addJava "${1:2}" && shift ;;
              *) addResidual "$1" && shift ;;
    esac
  done
  
  is_function_defined process_my_args && {
    myargs=("${residual_args[@]}")
    residual_args=()
    process_my_args "${myargs[@]}"
  }
}

# Detect that we have java installed.
checkJava() {
  local required_version="$1"
  # Now check to see if it's a good enough version
  declare -r java_version=$("$java_cmd" -version 2>&1 | awk -F '"' '/version/ {print $2}')
  if [[ "$java_version" == "" ]]; then
    echo
    echo No java installations was detected.
    echo Please go to http://www.java.com/getjava/ and download
    echo
    exit 1
  elif [[ ! "$java_version" > "$required_version" ]]; then
    echo
    echo The java installation you have is not up to date
    echo $script_name requires at least version $required_version+, you have
    echo version $java_version
    echo
    echo Please go to http://www.java.com/getjava/ and download
    echo a valid Java Runtime and install before running $script_name.
    echo
    exit 1
  fi
}


run() {
  # no jar? download it.
  [[ -f "$sbt_jar" ]] || acquire_sbt_jar "$sbt_version" || {
    # still no jar? uh-oh.
    echo "Download failed. Obtain the sbt-launch.jar manually and place it at $sbt_jar"
    exit 1
  }

  # process the combined args, then reset "$@" to the residuals
  process_args "$@"
  set -- "${residual_args[@]}"
  argumentCount=$#

  # TODO - java check should be configurable...
  checkJava "1.6"

  #If we're in cygwin, we should use the windows config, and terminal hacks
  if [[ "$CYGWIN_FLAG" == "true" ]]; then
    stty -icanon min 1 -echo > /dev/null 2>&1
    addJava "-Djline.terminal=jline.UnixTerminal"
    addJava "-Dsbt.cygwin=true"
  fi
  
  # run sbt
  execRunner "$java_cmd" \
    ${SBT_OPTS:-$default_sbt_opts} \
    $(get_mem_opts $sbt_mem) \
  	  ${java_opts} \
    ${java_args[@]} \
    -jar "$sbt_jar" \
    "${sbt_commands[@]}" \
    "${residual_args[@]}"  
  
  exit_code=$?

  # Clean up the terminal from cygwin hacks.
  if [[ "$CYGWIN_FLAG" == "true" ]]; then
    stty icanon echo > /dev/null 2>&1
  fi
  exit $exit_code
}

runAlternateBoot() {
  local bootpropsfile="$1"
  shift
  addJava "-Dsbt.boot.properties=$bootpropsfile"
  run $@
}
