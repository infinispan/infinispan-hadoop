## Infinispan Flink demo 

This folder contains a sample job and scripts to demonstrate usage of ```org.infinispan.hadoop.InfinispanInputFormat``` and ```org.infinispan.hadoop.InfinispanOutputFormat```
in other scenarios different from Hadoop MapReduce, by running an [Apache Flink](https://flink.apache.org/) job against data stored in the cache. 

### Requirements

* Linux or MacOS X
* Docker should be installed and running.Check it with ```docker --version```
* Samples built: run ```mvn clean install``` in the ```samples/``` directory 

### Note for MacOS users

Add a route so that containers can be reached via their IPs:

```
sudo route -n add 172.17.0.0/16 `docker-machine ip default`
```

### Preparing the Infinispan cluster

Run the script ```./run-clusters.sh``` to launch a two node Infinispan cluster and a two node Flink cluster. 

The Flink admin console can be found at:
 
```
[http://master:8081/](http://master:8081/)
```

### Populating the cache

A simple file with 1k random phrases can be generated using:

```
docker exec -it master /usr/local/sample/target/scripts/generate.sh 1000
```

Inspect it using:

```
docker exec -it master more /file.txt
```

and populate the cache using the command line:

```
docker exec -it master sh -c "java -cp /usr/local/sample/target/*dependencies.jar org.infinispan.hadoop.sample.util.ControllerCache --host ispn-1 --cachename phrases --populate --file /file.txt"
``` 
 
### Executing the job

To execute the job ```org.infinispan.hadoop.flink.sample.WordFrequency``` that reads data from the ```phrases``` cache and prints a histogram of the number of words per phrase:

```
docker exec -it master sh -c "/usr/local/flink/bin/flink run  /usr/local/sample/target/*dependencies.jar ispn-1"
```

### Changing the job

The ```master``` docker container automatically maps the current folder to ```/usr/local/sample/ ``` inside the container; should you want to change the job, it's enough to rebuild the uber jar and re-run the job to pick up changes

### Cleanup

To remove all the docker containers created in this sample:

```
docker-compose stop
```

