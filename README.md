# Infinispan Hadoop 

Integrations with Apache Hadoop and related frameworks.

### Requirements

* Java 8
* Infinispan Server 8.0.0.Final
* Hadoop Yarn 2.x

### InfinispanInputFormat and InfinispanOutputFormat

Implementation of Hadoop InputFormat and OutputFormat that allows reading and writing data to Infinispan Server with best data locality. 
Partitions are generated based on segment ownership and allows processing of data in a cache using multiple splits in parallel. 

### Maven Coordinates

```
 <dependency>  
    <groupId>org.infinispan.hadoop</groupId>  
    <artifactId>infinispan-hadoop-core</artifactId>  
    <version>0.1-SNAPSHOT</version>  
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
hadoop.ispn.input.remote.cache.host | Hostname or IP Address of the input cache | localhost
hadoop.ispn.input.remote.cache.port | HotRod server port of the input cache | 11222
hadoop.ispn.output.cache.name | The name of cache where job results will be written to | "default"
hadoop.ispn.output.remote.cache.host |  Hostname or IP Address of the output cache
hadoop.ispn.output.remote.cache.port | HotRod server port of the output cache | 11222
hadoop.ispn.input.converter | Class name with an implementation of ```org.infinispan.hadoop.KeyValueConverter```, applied after reading from the cache | null (no converting)
hadoop.ispn.output.converter | Class name with an implementation of ```org.infinispan.hadoop.KeyValueConverter```, applied before writing | null (no converting)

#### Demos

Refer to https://github.com/infinispan/infinispan-hadoop/tree/master/samples/mapreduce/

