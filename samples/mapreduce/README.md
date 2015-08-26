## Infinispan Hadoop Map Reduce demo 

This folder contains a sample job and scripts to demonstrate usage of ```org.infinispan.hadoop.InfinispanInputFormat``` and ```org.infinispan.hadoop.InfinispanOutputFormat```
by running a YARN Map Reduce job against data stored in the cache.

### Requirements

* Docker should be installed  
* Samples uberjar built: run ```mvn clean install``` to produce ```target/mapreduce-sample-VERSION-jar-with-dependencies.jar``` 

### Preparing the Infinispan cluster

The script ```scripts/create-ispn-cluster.sh``` will create a 2 node server cluster each one with two distributed caches: ```map-reduce-in``` and ```map-reduce-out```

After successful creation, it should print:

```
Cluster created. Server1 @ 172.17.0.23, Server2 @ 172.17.0.24
```

For the rest of the demo, store the Infinispan server host in a variable for convenience:

```
export INFINISPAN_SERVER=172.17.0.23
```

### Preparing the YARN cluster

To create a 3 node YARN cluster:

```
bash <(curl -s https://raw.githubusercontent.com/gustavonalle/docker/master/yarn/cluster.sh)
```

After successful creation, it should print:

```
Cluster started. HDFS UI on http://172.17.0.26:50070/dfshealth.html#tab-datanode
YARN UI on http://172.17.0.26:8088/cluster/nodes
```

### Populating the cache

A simple 100k text file with random phrases can be generated using:

```docker exec -it master  /usr/local/sample/scripts/generate.sh 100000```

Inspect it using:

```
docker exec -it master more /file.txt
```

and populate the cache using the command line:

```
docker exec -it master java -cp /usr/local/sample/target/mapreduce-sample-0.1-SNAPSHOT-jar-with-dependencies.jar  org.infinispan.hadoop.util.ControllerCache --host $INFINISPAN_SERVER --cachename map-reduce-in --populate --file /file.txt
``` 
 
### Executing the job

The job will read data from ```map-reduce-in``` cache, count words and write the output to ```map-reduce-out```:

```
docker exec -it master sh -l yarn jar /usr/local/sample/target/mapreduce-sample-0.1-SNAPSHOT-jar-with-dependencies.jar org.infinispan.hadoop.sample.InfinispanJobMain $INFINISPAN_SERVER
```

### Dump the output

```
docker exec -it master java -cp /usr/local/sample/target/mapreduce-sample-0.1-SNAPSHOT-jar-with-dependencies.jar org.infinispan.hadoop.util.ControllerCache --host $INFINISPAN_SERVER --cachename map-reduce-out --dump
```

### Changing the job

The ```master``` docker container automatically maps the current folder to ```/usr/local/sample/ ``` inside the container; should you want to change the job, it's enough to rebuild the uber jar and re-run the job to pick up changes

### Cleanup

To kill all the docker containers created in this sample:

```
docker kill slave1 slave2 master ispn-1 ispn-2
```

