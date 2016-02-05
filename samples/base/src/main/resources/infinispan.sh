#!/usr/bin/env bash

function create-cache()
{
   docker exec -ti $1 /opt/jboss/infinispan-server/bin/jboss-cli.sh -c command="/subsystem=datagrid-infinispan/cache-container=clustered/configurations=CONFIGURATIONS/distributed-cache-configuration=$2:add(start=EAGER,mode=SYNC)"
   docker exec -ti $1 /opt/jboss/infinispan-server/bin/jboss-cli.sh -c command="/subsystem=datagrid-infinispan/cache-container=clustered/distributed-cache=$2:add(configuration=$2)"
}

function ip()
{
  echo "$(docker inspect --format '{{ .NetworkSettings.IPAddress }}' $1)"
}

function fix-permissions()
{
  docker exec -ti $1 chmod 755 /opt/jboss/infinispan-server/bin/jboss-cli.sh
}

function launch-server()
{
  local server="$(docker run -d -it --name $1 gustavonalle/infinispan-server)"
  fix-permissions $1
  echo $(ip ${server})
}

function get_variable() {
   mvn org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=$1 | grep -v '\[.*\]'
}

function waitForCluster()
{
  MEMBERS=''
  while [ "$MEMBERS" != \"2\" ];
  do
    MEMBERS=$(docker exec -ti $1 /opt/jboss/infinispan-server/bin/jboss-cli.sh -c "/subsystem=datagrid-infinispan/cache-container=clustered:read-attribute(name=cluster-size)" | grep result | tr -d '\r' | awk '{print $3}')
    echo "Waiting for cluster to form (current members: $MEMBERS )"
    sleep 5
  done
}

