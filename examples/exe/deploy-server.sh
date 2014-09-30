#!/bin/bash
#
# Usage:
# 	% deploy-server.sh
# or
#	% DST_USER=root DST_HOST=canary.adatao.com deploy-server.sh
#
DST_USER=${DST_USER-root}
DST_HOST=${DST_HOST-canary.adatao.com}

ARTIFACT_ID="rserver"
SRC_DIR="$HOME/deploy"
DST_URL="${DST_USER}@${DST_HOST}"
DST_DIR="/root/deploy"

set -x
ssh "$DST_URL" mkdir -p "${DST_DIR}/${ARTIFACT_ID}"
rsync -av --delete "${SRC_DIR}/${ARTIFACT_ID}/" "${DST_URL}:${DST_DIR}/${ARTIFACT_ID}/"
