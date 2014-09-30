#######################################################
# (c) Copyright 2014, Adatao, Inc. All Rights Reserved.

.onLoad <- function(libname, pkgname) {
  #options( java.parameters = "-Xmx2000m" )
  .jinit(parameters="-Dlog4j.configuration=ddf-local-log4j.properties")
  .jpackage(pkgname, lib.loc = libname)
  packageStartupMessage("ddf - for fast, easy and transparent Big Data processing in R")
}
