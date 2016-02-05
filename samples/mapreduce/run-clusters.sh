#!/usr/bin/env bash

set -e

CWD="$(dirname "$0")"
. $CWD/target/scripts/infinispan.sh

command -v docker-compose >/dev/null 2>&1 || { echo >&2 "docker-compose not installed.  Aborting."; exit 1; }

INFINISPAN_VERSION=$(get_variable "version.infinispan")
YARN_VERSION=$(get_variable "version.hadoop")
export INFINISPAN_VERSION YARN_VERSION

docker-compose up -d

waitForCluster "ispn-1"
echo "Infinispan cluster started."

echo "Creating caches"
create-cache "ispn-1" "map-reduce-in"
create-cache "ispn-1" "map-reduce-out"

create-cache "ispn-2" "map-reduce-in"
create-cache "ispn-2" "map-reduce-out"

echo "Starting YARN cluster"
chmod 755 target/scripts/*.sh
docker exec -it master sh -c -l '/usr/local/hadoop/sbin/start-wrapper.sh'