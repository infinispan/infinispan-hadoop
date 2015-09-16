## Infinispan Flink demo 

This folder contains a sample job and scripts to demonstrate usage of ```org.infinispan.hadoop.InfinispanInputFormat``` and ```org.infinispan.hadoop.InfinispanOutputFormat```
in other scenarios different from Hadoop MapReduce, by running an [Apache Flink](https://flink.apache.org/) job against data stored in the cache. 

### Requirements

* Linux or MacOS X (using boot2docker)
* Docker should be installed and running.Check it with ```docker --version```
* Samples built: run ```mvn clean install``` in the ```samples/``` directory 

### Note for boot2docker users

Add a route so that containers can be reached via their IPs:

```
sudo route -n add 172.17.0.0/16 `boot2docker ip`
```

### Preparing the Infinispan cluster

Inside ```flink/```, prepare the infinispan cluster:

The script ```scripts/create-ispn-cluster.sh``` will create a 2 node server cluster with a distributed cache called ```phrases```

After successful creation, it should print:

```
Cluster created. Server1 @ 172.17.0.23, Server2 @ 172.17.0.24
```

### Exporting the server location

For convenience, export a variable INFINISPAN_SERVER with the IP of the server:

```
export INFINISPAN_SERVER=$(docker inspect --format '{{ .NetworkSettings.IPAddress }}' ispn-1)
```

### Preparing the Apache Flink cluster

To create a 3 node Flink cluster:

```
bash <(curl -s https://raw.githubusercontent.com/gustavonalle/docker/master/flink/cluster.sh)
```

After successful creation, it should print:

```
Cluster started. Cluster started. Dashboard on http://172.17.0.5:8081/
```

### Populating the cache

A simple 1k lines text file with random phrases can be generated using:

```docker exec -it flink-master /usr/local/sample/target/scripts/generate.sh 1000```

Inspect it using:

```
docker exec -it flink-master more /file.txt
```

and populate the cache using the command line:

```
docker exec -it flink-master java -cp /usr/local/sample/target/flink-sample-0.1-SNAPSHOT-jar-with-dependencies.jar  org.infinispan.hadoop.sample.util.ControllerCache --host $INFINISPAN_SERVER --cachename phrases --populate --file /file.txt
``` 
 
### Executing the job

To execute the job ```org.infinispan.hadoop.flink.sample.WordFrequency``` that reads data from the ```phrases``` cache and prints a histogram of the number of words per phrase:

```
docker exec -it flink-master /usr/local/flink/bin/flink run  /usr/local/sample/target/flink-sample-0.1-SNAPSHOT-jar-with-dependencies.jar $INFINISPAN_SERVER
```

### Changing the job

The ```flink-master``` docker container automatically maps the current folder to ```/usr/local/sample/ ``` inside the container; should you want to change the job, it's enough to rebuild the uber jar and re-run the job to pick up changes

### Cleanup

To remove all the docker containers created in this sample:

```
docker rm -f ispn-1 ispn-2 flink-master flink-slave1 flink-slave2
```

