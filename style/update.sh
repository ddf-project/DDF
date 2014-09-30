#!/bin/bash
#
# Update Eclipse formatters/templates from central source. Makes certain assumptions
# about where things are, so this is not guaranteed to work for everyone. Only
# here for convenience of some maintainers.
#

GITROOT=git@github.com:adatao
SRC=projects/docs/code-convention
DST=BigR/eclipse
ROOT=../..

FILES=($ROOT/$SRC/*)

for file in ${FILES[*]} ; do
  base=`basename $file`
  echo $base
  cp $file .
done

# Special handling for README.md
cat > README.md << END
### ATTENTION: files in this directory are automatically copied from ${GITROOT}/${SRC}. If you have modifiations, make them there. Modifications made here will be overwritten.

Generated: `date` by `whoami`@`hostname`

END

cat $ROOT/$SRC/README.md >> README.md
