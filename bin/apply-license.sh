#!/bin/bash


usage() {
	[ "$@" ] && echo "Error: $@"
	echo "Usage: $0"
	exit 1
}

preamble() {
	# Make sure we're in the src/ directory
	local found=false
	for i in `seq 1 20`; do
		[ -d src/main ] && found=true && break
	done
	[ $found != true ] && usage "Cannot locate src/ directory"
}

apply_license() {
	local file=$1 ; [ "$1" ] || usage "Missing <file> argument to apply_license()"
	[ ! -f $file ] && usage "File $file not found"
	grep "Licensed under the Apache License" $file >/dev/null && echo "> File $PWD/$file already has license" && return

	echo "> Applying license to $PWD/$file"
	local temp=/tmp/$file.$$
	cat >$temp <<END
/*
 *  Copyright (C) 2013 Adatao, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

END
	cat $file >> $temp
	cp $temp $file
	rm -f $temp
}

apply_license_to_all_files() {
	local pattern=$1 ; [ -z "$pattern" ] && pattern='.*'

	# Work on this dir
	for file in *.java *.scala ; do
		[ ! -f $file ] && continue
		[[ $file =~ $pattern ]] && apply_license $file
	done

	# Work on all subdirs
	for dir in */ ; do
		[ ! -d $dir ] && continue
		cd $dir
		apply_license_to_all_files $@
		cd ..
	done
}

run() {
	preamble $@
	apply_license_to_all_files $@
}

run $@
