package org.infinispan.hadoop.impl;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.infinispan.hadoop.KeyValueConverter;

import java.io.IOException;

/**
 * A decorator RecordReader capable of converting keys and values.
 *
 * @param <K1> original Key type
 * @param <V1> original Value type
 * @param <K>  Converted Key type
 * @param <V>  Converted Value type
 * @author gustavonalle
 * @since 0.1
 */
public class ConvertingRecordReader<K1, V1, K, V> extends RecordReader<K, V> {

   private final RecordReader<K1, V1> recordReader;
   private final KeyValueConverter<K1, V1, K, V> keyValueConverter;

   public ConvertingRecordReader(RecordReader<K1, V1> recordReader, KeyValueConverter<K1, V1, K, V> keyValueConverter) {
      this.recordReader = recordReader;
      this.keyValueConverter = keyValueConverter;
   }

   @Override
   public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
      recordReader.initialize(split, context);
   }

   @Override
   public boolean nextKeyValue() throws IOException, InterruptedException {
      return recordReader.nextKeyValue();
   }

   @Override
   public K getCurrentKey() throws IOException, InterruptedException {
      return keyValueConverter.convertKey(recordReader.getCurrentKey());
   }

   @Override
   public V getCurrentValue() throws IOException, InterruptedException {
      return keyValueConverter.convertValue(recordReader.getCurrentValue());
   }

   @Override
   public float getProgress() throws IOException, InterruptedException {
      return recordReader.getProgress();
   }

   @Override
   public void close() throws IOException {
      recordReader.close();
   }
}
