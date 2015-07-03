#!/usr/bin/env bash

export DDF_HOME=`pwd`/../../

make html

make latexpdf

if [ $? -eq 0 ]; then
    cp ../docs-build/latex/pyddf.pdf ../
    printf "\nDone. Check ../pyddf.pdf for the PDF documentation.\n"
fi
