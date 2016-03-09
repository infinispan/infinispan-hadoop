# Infinispan Hadoop   [![Build Status](https://travis-ci.org/infinispan/infinispan-hadoop.svg?branch=master)](https://travis-ci.org/infinispan/infinispan-hadoop)

Integrations with Apache Hadoop and related frameworks.

### Requirements

* Java 8
* Infinispan Server 8.2.0.Final or above
* Hadoop Yarn 2.x

### InfinispanInputFormat and InfinispanOutputFormat

Implementation of Hadoop InputFormat and OutputFormat that allows reading and writing data to Infinispan Server with best data locality. 
Partitions are generated based on segment ownership and allows processing of data in a cache using multiple splits in parallel. 

### Maven Coordinates

```
 <dependency>  
    <groupId>org.infinispan.hadoop</groupId>  
    <artifactId>infinispan-hadoop-core</artifactId>  
    <version>0.2</version>  
 </dependency>  
```

#### Sample usage with Hadoop YARN mapreduce application:

```
import org.infinispan.hadoop.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

Configuration configuration = new Configuration();

// Configures input/output caches
configuration.set(InfinispanConfiguration.INPUT_REMOTE_CACHE_HOST, host);
configuration.set(InfinispanConfiguration.OUTPUT_REMOTE_CACHE_HOST, host);

configuration.set(InfinispanConfiguration.INPUT_REMOTE_CACHE_NAME, "map-reduce-in");
configuration.set(InfinispanConfiguration.OUTPUT_REMOTE_CACHE_NAME, "map-reduce-out");

Job job = Job.getInstance(configuration, "Infinispan job");

// Map and Reduce implementation
job.setMapperClass(MapClass.class);
job.setReducerClass(ReduceClass.class);

job.setInputFormatClass(InfinispanInputFormat.class);
job.setOutputFormatClass(InfinispanOutputFormat.class);

```

#### Supported Configurations:

Name          | Description | Default
------------- | -------------|----------
hadoop.ispn.input.filter.factory  | The name of the filter factory deployed on the server to pre-filter data before reading | null (no filtering)
hadoop.ispn.input.cache.name  | The name of cache where data will be read from | "default"
hadoop.ispn.input.read.batch | Batch size when reading from the cache | 5000
hadoop.ispn.output.write.batch | Batch size when writing to the cache | 500
hadoop.ispn.input.remote.cache.servers | List of servers of the input cache, in the format ```host1:port1;host2:port2``` | localhost:11222
hadoop.ispn.output.cache.name | The name of cache where job results will be written to | "default"
hadoop.ispn.output.remote.cache.servers |  List of servers of the output cache, in the format ```host1:port1;host2:port2```
hadoop.ispn.input.converter | Class name with an implementation of ```org.infinispan.hadoop.KeyValueConverter```, applied after reading from the cache | null (no converting)
hadoop.ispn.output.converter | Class name with an implementation of ```org.infinispan.hadoop.KeyValueConverter```, applied before writing | null (no converting)

#### Demos

Refer to https://github.com/infinispan/infinispan-hadoop/tree/master/samples/

