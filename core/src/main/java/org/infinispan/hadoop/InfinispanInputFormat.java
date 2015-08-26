package org.infinispan.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.hadoop.impl.ConvertingRecordReader;
import org.infinispan.hadoop.impl.InfinispanCache;
import org.infinispan.hadoop.impl.InfinispanRecordReader;

import java.io.IOException;
import java.util.List;

/**
 * InputFormat for Infinispan remote caches, with best data locality.
 *
 * @author gustavonalle
 * @since 0.1
 */
public class InfinispanInputFormat<K, V> extends InputFormat<K, V> {

   @Override
   public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException {
      Configuration configuration = jobContext.getConfiguration();
      InfinispanConfiguration infinispanConfiguration = new InfinispanConfiguration(configuration);
      InfinispanCache<K, V> inputCache = InfinispanCache.getInputCache(infinispanConfiguration);
      InfinispanSplitter splitter = infinispanConfiguration.getSplitter();
      return splitter.calculateSplits(inputCache.getCacheTopology());
   }

   @Override
   public RecordReader<K, V> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
      Configuration configuration = taskAttemptContext.getConfiguration();
      InfinispanConfiguration infinispanConfiguration = new InfinispanConfiguration(configuration);
      String inputFilterFactory = infinispanConfiguration.getInputFilterFactory();
      Integer readBatchSize = infinispanConfiguration.getReadBatchSize();
      InfinispanCache<K, V> inputCache = InfinispanCache.getInputCache(infinispanConfiguration);
      RemoteCache<K, V> remoteCache = inputCache.getRemoteCache();
      KeyValueConverter<Object, Object, K, V> inputConverter = infinispanConfiguration.getInputConverter();
      InfinispanRecordReader<K, V> recordReader = new InfinispanRecordReader<>(remoteCache, inputFilterFactory, readBatchSize);
      if (inputConverter == null) {
         return recordReader;
      }
      return new ConvertingRecordReader<>((RecordReader<Object, Object>) recordReader, inputConverter);
   }

}
