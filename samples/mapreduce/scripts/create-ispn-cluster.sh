#!/usr/bin/env bash

set -e

function create-cache()
{
   docker exec -ti $1 /opt/jboss/infinispan-server/bin/jboss-cli.sh -c command="/subsystem=datagrid-infinispan/cache-container=clustered/configurations=CONFIGURATIONS/distributed-cache-configuration=$2:add(start=EAGER,mode=SYNC)"
   docker exec -ti $1 /opt/jboss/infinispan-server/bin/jboss-cli.sh -c command="/subsystem=datagrid-infinispan/cache-container=clustered/distributed-cache=$2:add(configuration=map-reduce-in)"
}

function ip()
{
  echo "$(docker inspect --format '{{ .NetworkSettings.IPAddress }}' $1)"
}

function chmod()
{
  docker exec -ti $1 chmod 755 /opt/jboss/infinispan-server/bin/jboss-cli.sh
}

function waitForCluster()
{
  MEMBERS=''
  while [ "$MEMBERS" != \"2\" ];
  do
    MEMBERS=$(docker exec -ti ispn-1 /opt/jboss/infinispan-server/bin/jboss-cli.sh -c "/subsystem=datagrid-infinispan/cache-container=clustered:read-attribute(name=cluster-size)" | grep result | tr -d '\r' | awk '{print $3}')
    echo "Waiting for cluster to form (current members: $MEMBERS )"
    sleep 5
  done
}


SERVER1="$(docker run -d -it --name ispn-1 gustavonalle/infinispan-server)"
SERVER2="$(docker run -d -it --name ispn-2 gustavonalle/infinispan-server)"
chmod "ispn-1"
chmod "ispn-2"
SERVER1_IP=$(ip $SERVER1)
SERVER2_IP=$(ip $SERVER2)
echo "Server 1 spawned, id=$SERVER1"
echo "Server 2 spawned, id=$SERVER2"

waitForCluster

echo "Creating caches"
create-cache "ispn-1" "map-reduce-in"
create-cache "ispn-1" "map-reduce-out"

create-cache "ispn-2" "map-reduce-in"
create-cache "ispn-2" "map-reduce-out"

echo "Cluster created. Server1 @ $SERVER1_IP, Server2 @ $SERVER2_IP"
