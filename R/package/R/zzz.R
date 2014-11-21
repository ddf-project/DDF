#######################################################
# (c) Copyright 2014, Adatao, Inc. All Rights Reserved.

.onLoad <- function(libname, pkgname) {
  options( java.parameters = c("-Xmx2000m", paste0("-Dlog4j.configuration=file:",libname,"/",pkgname,"/conf/local/ddf-local-log4j.properties")) )
  .jpackage(pkgname, lib.loc = libname)
  packageStartupMessage("ddf - for fast, easy and transparent Big Data processing in R")
}
