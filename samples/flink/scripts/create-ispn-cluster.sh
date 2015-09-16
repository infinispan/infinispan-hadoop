#!/usr/bin/env bash

set -e

CWD="$(dirname "$0")"
. $CWD/../target/scripts/infinispan.sh

SERVER1_IP=$(launch-server "ispn-1")
echo "Server 1 spawned"
SERVER2_IP=$(launch-server "ispn-2")
echo "Server 2 spawned"

waitForCluster "ispn-1"

echo "Creating caches"
create-cache "ispn-1" "phrases"
create-cache "ispn-2" "phrases"

chmod 755 target/scripts/*.sh

echo "Cluster created. Server1 @ $SERVER1_IP, Server2 @ $SERVER2_IP"