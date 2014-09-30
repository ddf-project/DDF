This directory contains jars that we need to include (such as patched Hive) in order to complete the build.
These jars are "unmanaged" in that they're not available for download from any well-known repos.
Thus we include a script, mvn-install-jars.sh, that helps install these jars into the local mvn repo.
