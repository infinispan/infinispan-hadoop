#!/usr/bin/env bash

set -e

CWD="$(dirname "$0")"
. $CWD/target/scripts/infinispan.sh

command -v docker-compose >/dev/null 2>&1 || { echo >&2 "docker-compose not installed.  Aborting."; exit 1; }

INFINISPAN_VERSION=$(get_variable "version.infinispan")
FLINK_VERSION=$(get_variable "version.flink")
export INFINISPAN_VERSION FLINK_VERSION

docker network create sample || echo "Not creating network as it already exists"
docker-compose up -d

waitForCluster "ispn-1"
echo "Infinispan cluster started."

echo "Creating caches"
create-cache "ispn-1" "phrases"
create-cache "ispn-2" "phrases"

echo "Starting Flink cluster"
chmod 755 target/scripts/*.sh
docker exec master /usr/local/flink/bin/start-cluster-wrapper.sh
