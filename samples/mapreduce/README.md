## Infinispan Hadoop Map Reduce demo 

This folder contains a sample job and scripts to demonstrate usage of ```org.infinispan.hadoop.InfinispanInputFormat``` and ```org.infinispan.hadoop.InfinispanOutputFormat```
by running a YARN Map Reduce job against data stored in the cache.

### Requirements

* Linux or MacOS X
* Docker should be installed and running.Check it with ```docker --version```  
* Samples built: run ```mvn clean install``` in the ```samples/``` directory

### Note for MacOS users

Add a route so that containers can be reached via their IPs directly:

```
sudo route -n add 172.17.0.0/16 `docker-machine ip default`
```

### Launching the clusters the Infinispan cluster

Run the script ```run-clusters.sh``` to launch a two node Infinispan cluster and a two node YARN cluster. 

The YARN admin interfaces can be found at:

[http://master:50070/dfshealth.html#tab-datanode](http://master:50070/dfshealth.html#tab-datanode)   

and  

[http://master:8088/cluster/nodes](http://master:8088/cluster/nodes)

### Populating the cache

A simple text file with 100k random phrases can be generated using:

```
docker exec -it master /usr/local/sample/target/scripts/generate.sh 100000
```

Inspect it using:

```
docker exec -it master more /file.txt
```

and populate the cache using the command line:

```
docker exec -it master sh -c "java -cp /usr/local/sample/target/*dependencies.jar  org.infinispan.hadoop.sample.util.ControllerCache --host ispn-1 --cachename map-reduce-in --populate --file /file.txt"
``` 
 
### Executing the job

To execute the Job ```org.infinispan.hadoop.sample.InfinispanJobMain``` that reads data from the ```map-reduce-in``` cache, count words and write the output to ```map-reduce-out```:

```
docker exec -it master sh -l -c "yarn jar /usr/local/sample/target/*dependencies.jar org.infinispan.hadoop.sample.InfinispanJobMain ispn-1;ispn-2"
```

### Dump the output

```
docker exec -it master sh -c "java -cp /usr/local/sample/target/*dependencies.jar org.infinispan.hadoop.sample.util.ControllerCache --host ispn-1 --cachename map-reduce-out --dump | more"
```

### Changing the job

The ```master``` docker container automatically maps the current folder to ```/usr/local/sample/ ``` inside it; should you want to change the job, it's enough to rebuild the uber jar and re-run the job to pick up changes

### Cleanup

To remove all the docker containers created in this sample:

```
docker-compose stop
```

