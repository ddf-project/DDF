#!/bin/bash
#
# Helps refactor both Scala and Java code base, with some common refactoring operations.
# Too bad the Scala Eclipse refactor tool is quite buggy, necessitating this.
#
# Experimental! Always check your results before checking in. If necessary, remember
# "git reset --hard" is your friend.
#
# Operations we support:
#
#  * Move/Rename an entire package
#  * Move a class from one package to another
#  * Rename a class

usage() {
	echo ""
	[ -n "$@" ] && echo "Error: $@
"
	echo "Usage: $0 <operation> [args]

	where operations are:

	- move-package|mp <from_package> <to_package>
	- move-class|mc <class_name> <from_package> <to_package> (does not change class name)
"
#	- rename-class|rc <package_name> <from_class> <to_class> (does not change package)

	exit 1
}


preamble() {
	# Make sure we are in the src/ directory
	local found=false
	for i in `seq 1 20` ; do
		[ -d src/main ] && found=true && break
		cd ..
	done
	[ $found == false ] && usage "Cannot locate src/main directory, please cd to containing directory first."
	cd src
	echo "++++ Operating in $PWD"
}


run() {
	preamble

	local operation=$1 ; [ -z "$1" ] && usage "Missing operation" ; shift

	case $operation in
		"move-package"|"mp")
			move_package $@
			;;

		"move-class"|"mc")
			move_class $@
			;;

		"rename-class"|"rc")
			usage "Sorry, not yet supported"
			;;
		
		*)
			usage
			;;
	esac
}


package_to_dir() {
	echo $1 | tr . /
}


find_package_dirs() {
	local package_name=$1 ; [ -z "$1" ] && usage "Missing package_name argument to find_package_dirs()"
	local package_dirs="`echo {main,test}/{java,scala}/$package_name`"
	local result
	for dir in $package_dirs ; do
		[ -d $dir ] && result+="$dir "
	done
	echo $result
}


is_empty_dir() {
	local dir=$1 ; [ -z "$1" ] && usage "Missing <dir> argument to git_mv_dir()" ; shift
	[ ! "$(ls $dir)" ]
}

git_mv_dir() {
	local from_dir=$1 ; [ -z "$1" ] && usage "Missing <from_dir> argument to git_mv_dir()" ; shift
	local to_dir=$1 ; [ -z "$1" ] && usage "Missing <to_dir> argument to git_mv_dir()" ; shift

	# Recursively handle subdirs FIRST, since we can only move files, not dirs
	local cwd=$PWD
	cd $from_dir
	for subdir in */ ; do
		[ -d $from_dir/$subdir ] && git_mv_dir $from_dir/$subdir $to_dir/$subdir
	done
	cd $cwd

	# Now handle the parent dir
	is_empty_dir $from_dir && echo "Nothing to do" && return # No files to move
	mkdir -p $to_dir
	git mv $from_dir/* $to_dir
}


move_package() {
	local from_package=$1 ; [ -z "$1" ] && usage "Missing from_package" ; shift
	local to_package=$1 ; [ -z "$1" ] && usage "Missing to_package" ; shift
	echo ++++ Moving package $from_package to $to_package

	local from_package_dir=`package_to_dir $from_package`
	local to_package_dir=`package_to_dir $to_package`

	# Make sure source and destination packages exist
	local from_package_dirs="`find_package_dirs $from_package_dir`"
	[ -z "$from_package_dirs" ] && usage "No matching source package named '$from_package' found."
	local to_package_dirs=`echo $from_package_dirs | sed -e "s/$from_package/$to_package/g" -e "s/\./\//g"`
	[ "$from_package_dirs" == "$to_package_dirs" ] && usage "No change in moving from $from_package to $to_package"

	# Git mv the entire directory
	local from_dirs=($from_package_dirs)
	local to_dirs=($to_package_dirs)
	local i=0
	for i in `seq 0 $(( ${#from_dirs[@]} - 1 ))` ; do
		git_mv_dir ${from_dirs[$i]} ${to_dirs[$i]}
	done

	# Scan files for any old package references, and change them to the new package name
	modify_imports $from_package $to_package

	# Remove unnecessary (empty) to_package_dirs
	for dir in $to_package_dirs ; do
		is_empty_dir $dir && echo "   > Removing unnecessary $dir" && rm -fr $dir
	done
}


modify_imports() {
	local from_pattern=$1 ; [ -z "$1" ] && usage "Missing <from_pattern> parameter to modify_imports()" ; shift
	local to_pattern=$1 ; [ -z "$1" ] && usage "Missing <to_pattern> parameter to modify_imports()" ; shift
	local file_list=`egrep -l -r "^(import|package)[ 	]+$from_pattern" .`
	for file in $file_list ; do
		echo "   > Updating import references in $file"
		sed -i "" -e "s/^import $from_pattern/import $to_pattern/g" -e "s/^package $from_pattern/package $to_pattern/g" $file
	done
}


move_class() {
	local class_name=$1 ; [ -z "$1" ] && usage "Missing class_name" ; shift
	local from_package=$1 ; [ -z "$1" ] && usage "Missing from_package" ; shift
	local to_package=$1 ; [ -z "$1" ] && usage "Missing to_package" ; shift
	echo ++++ Moving $class from $from_package to $to_package

	local from_package_dir=`package_to_dir $from_package`
	local to_package_dir=`package_to_dir $to_package`

	# Make sure source and destination packages exist
	local from_package_dirs="`find_package_dirs $from_package_dir`"
	[ -z "$from_package_dirs" ] && usage "No matching source package named '$from_package' found."
	local to_package_dirs=`echo $from_package_dirs | sed -e "s/$from_package/$to_package/g" -e "s/\./\//g"`
	[ "$from_package_dirs" == "$to_package_dirs" ] && usage "No change in moving from $from_package to $to_package"

	# Move the file itself
	local from_dirs=($from_package_dirs)
	local to_dirs=($to_package_dirs)
	local i=0
	for i in `seq 0 $(( ${#from_dirs[@]} - 1 ))` ; do
		for file in ${from_dirs[$i]}/${class_name}* ; do
			[ ! -f $file ] && continue
			sed -i "" -e "s/^package $from_package/package $to_package/g" $file
			mkdir -p ${to_dirs[$i]} ; git mv $file ${to_dirs[$i]}
		done
	done

	# Modify import statements to point to the new package
	modify_imports $from_package.$class_name $to_package.$class_name

	# Remove unnecessary imports of the class when it's referred to by files in the destination package

	# Add required imports of the class when it's referred to by files in the source package
	# NB: this is broken---we can't really do this perfectly because we don't understand the language syntax.
	# The developer should fix this part manually.
	for i in `seq 0 $(( ${#from_dirs[@]} - 1 ))` ; do
		local from_dir=${from_dirs[$i]}
		local file_list=`egrep -l $class_name $from_dir/*` # find all files with local references to our class_name
		local temp_file=/tmp/import.$$
		for file in $file_list ; do
			[ ! -f $file ] && continue
			# Insert an import statement
			awk "/^import/ { if (!DONE) print \"import $to_package.$class_name\"; DONE=1 } { print }" $file > $temp_file
			cp $temp_file $file
			rm -f $temp_file
		done
	done
}


# This is hard because it involves understanding the syntax where class names are referenced in the code itself
rename_class() {
	local package_name=$1 ; [ -z "$1" ] && usage "Missing package_name" ; shift
	local from_class=$1 ; [ -z "$1" ] && usage "Missing from_class" ; shift
	local to_class=$1 ; [ -z "$1" ] && usage "Missing to_class" ; shift
	echo ++++ Renaming $from_class to $to_class in $package
}


run $@
