package org.infinispan.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.infinispan.hadoop.impl.ConvertingRecordWriter;
import org.infinispan.hadoop.impl.InfinispanCache;
import org.infinispan.hadoop.impl.InfinispanRecordWriter;

import java.io.IOException;

/**
 * Output format that saves data to a Infinispan remote cache.
 *
 * @author gustavonalle
 * @since 0.1
 */
public class InfinispanOutputFormat<K, V> extends OutputFormat<K, V> {

   @Override
   public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
      Configuration configuration = context.getConfiguration();
      InfinispanConfiguration infinispanConfiguration = new InfinispanConfiguration(configuration);
      Integer writeBatchSize = infinispanConfiguration.getWriteBatchSize();
      KeyValueConverter<Object, Object, K, V> outputConverter = infinispanConfiguration.getOutputConverter();
      InfinispanCache<K, V> outputCache = InfinispanCache.getOutputCache(infinispanConfiguration);
      InfinispanRecordWriter<K, V> recordWriter = new InfinispanRecordWriter<>(outputCache.getRemoteCache(), writeBatchSize);
      if (outputConverter == null) {
         return recordWriter;
      }
      return new ConvertingRecordWriter<>((RecordWriter<Object, Object>) recordWriter, outputConverter);
   }

   @Override
   public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
   }

   @Override
   public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
      return new OutputCommitter() {
         @Override
         public void setupJob(JobContext jobContext) throws IOException {
         }

         @Override
         public void setupTask(TaskAttemptContext taskContext) throws IOException {
         }

         @Override
         public boolean needsTaskCommit(TaskAttemptContext taskContext) throws IOException {
            return false;
         }

         @Override
         public void commitTask(TaskAttemptContext taskContext) throws IOException {
         }

         @Override
         public void abortTask(TaskAttemptContext taskContext) throws IOException {
         }
      };
   }
}
