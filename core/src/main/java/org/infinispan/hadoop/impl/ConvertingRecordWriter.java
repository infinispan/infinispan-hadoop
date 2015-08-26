package org.infinispan.hadoop.impl;

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.infinispan.hadoop.KeyValueConverter;

import java.io.IOException;

public class ConvertingRecordWriter<K, V> extends RecordWriter<K, V> {

   private final RecordWriter<Object, Object> recordWriter;
   private final KeyValueConverter<Object, Object, K, V> keyValueConverter;

   public ConvertingRecordWriter(RecordWriter<Object, Object> recordWriter, KeyValueConverter<Object, Object, K, V> keyValueConverter) {
      this.recordWriter = recordWriter;
      this.keyValueConverter = keyValueConverter;
   }

   @Override
   public void write(K key, V value) throws IOException, InterruptedException {
      recordWriter.write(keyValueConverter.convertKey(key), keyValueConverter.convertValue(value));
   }

   @Override
   public void close(TaskAttemptContext context) throws IOException, InterruptedException {
      recordWriter.close(context);
   }
}
