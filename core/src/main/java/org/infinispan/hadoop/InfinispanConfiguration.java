package org.infinispan.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.hadoop.impl.DefaultSplitter;
import org.infinispan.hadoop.serialization.JBossMarshallerSerialization;

import java.io.IOException;

/**
 * Configurations for the {@link InfinispanInputFormat} and {@link InfinispanOutputFormat}.
 *
 * @author Pedro Ruivo
 * @author gustavonalle
 * @since 0.1
 */
public final class InfinispanConfiguration {

   public static final int DEFAULT_READ_BATCH_SIZE = 5000;
   public static final int DEFAULT_WRITE_BATCH_SIZE = 500;
   public static final String DEFAULT_SERVER_LIST = "127.0.0.1:11222";

   /**
    * Name of the filter factory deployed in the server to be used as a pre-filter in the mapper
    */
   public static final String INPUT_FILTER_FACTORY = "hadoop.ispn.input.filter.factory";

   /**
    * Name of the cache where map input data is located
    */
   public static final String INPUT_REMOTE_CACHE_NAME = "hadoop.ispn.input.cache.name";

   /**
    * Initial list of Hot Rod servers to connect to input cache, specified in the following format:
    * host1:port1;host2:port2...
    * At least one host:port must be specified
    */
   public static final String INPUT_REMOTE_CACHE_SERVER_LIST = "hadoop.ispn.input.remote.cache.servers";

   /**
    * Name of cache where output from the reducer will be stored
    */
   public static final String OUTPUT_REMOTE_CACHE_NAME = "hadoop.ispn.output.cache.name";

   /**
    * Initial list of Hot Rod servers to connect to output cache, specified in the following format:
    * host1:port1;host2:port2...
    * At least one host:port must be specified
    */
   public static final String OUTPUT_REMOTE_CACHE_SERVER_LIST = "hadoop.ispn.output.remote.cache.servers";

   /**
    * Comma separated list of classes whose serialization is to be handled by {@link
    * JBossMarshallerSerialization}
    */
   public static final String SERIALIZATION_CLASSES = "hadoop.ispn.io.serialization.classes";

   /**
    * Batch size to read entries from the cache
    */
   public static final String INPUT_READ_BATCH_SIZE = "hadoop.ispn.input.read.batch";

   /**
    * Batch size to write entries from the cache
    */
   public static final String OUTPUT_WRITE_BATCH_SIZE = "hadoop.ispn.output.write.batch";

   /**
    * Optional input converter for reading from the cache before the map phase
    */
   public static final String INPUT_KEY_VALUE_CONVERTER = "hadoop.ispn.input.converter";

   /**
    * Optional output converter for writing to the cache after the reduce phase
    */
   public static final String OUTPUT_KEY_VALUE_CONVERTER = "hadoop.ispn.output.converter";

   /**
    * Optional implementation of {@link InfinispanSplitter}
    */
   public static final String SPLITTER_CLASS = "hadoop.ispn.input.splitter.class";

   private final Configuration configuration;

   public InfinispanConfiguration(Configuration configuration) {
      this.configuration = configuration;
   }

   public String getInputCacheName() {
      return configuration.get(INPUT_REMOTE_CACHE_NAME, RemoteCacheManager.DEFAULT_CACHE_NAME);
   }

   public String getOutputCacheName() {
      return configuration.get(OUTPUT_REMOTE_CACHE_NAME, RemoteCacheManager.DEFAULT_CACHE_NAME);
   }

   public String getInputRemoteCacheServerList() {
      return configuration.get(INPUT_REMOTE_CACHE_SERVER_LIST, DEFAULT_SERVER_LIST);
   }

   public String getOutputRemoteCacheServerList() {
      return configuration.get(OUTPUT_REMOTE_CACHE_SERVER_LIST, DEFAULT_SERVER_LIST);
   }

   public String getInputFilterFactory() {
      return configuration.get(INPUT_FILTER_FACTORY);
   }

   public Integer getReadBatchSize() {
      return configuration.getInt(INPUT_READ_BATCH_SIZE, DEFAULT_READ_BATCH_SIZE);
   }

   public Integer getWriteBatchSize() {
      return configuration.getInt(OUTPUT_WRITE_BATCH_SIZE, DEFAULT_WRITE_BATCH_SIZE);
   }

   public InfinispanSplitter getSplitter() throws IOException {
      String splitterClass = configuration.get(SPLITTER_CLASS);
      if (splitterClass == null) {
         return new DefaultSplitter();
      }
      return getInstance(splitterClass);
   }

   public <K1, K2, V1, V2> KeyValueConverter<K1, K2, V1, V2> getInputConverter() throws IOException {
      return getInstance(configuration.get(INPUT_KEY_VALUE_CONVERTER));
   }

   public <K1, K2, V1, V2> KeyValueConverter<K1, K2, V1, V2> getOutputConverter() throws IOException {
      return getInstance(configuration.get(OUTPUT_KEY_VALUE_CONVERTER));
   }

   @SuppressWarnings("unchecked")
   private <T> T getInstance(String className) throws IOException {
      if (className != null) {
         try {
            return (T) Class.forName(className).newInstance();
         } catch (Exception e) {
            throw new IOException(e);
         }
      }
      return null;
   }

}
