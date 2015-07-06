#!/usr/bin/env bash

# this only needs to run once
# rm -rf _build/ _static/ _templates/ *.rst conf.py Makefile make.bat
# sphinx-apidoc -f -F -M -H pyddf -A Adatao -V 1.2 -R 1.2.0 -o ./ ../ddf

# we made changes to Makefile and conf.py, so won't delete them
rm -rf _build/ _static/ _templates/ *.rst
sphinx-apidoc -M -F -H pyddf -A Adatao -o ./ ../ddf

