package org.infinispan.hadoop.impl;


import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.commons.util.CloseableIterator;

import java.io.IOException;
import java.util.Map.Entry;

/**
 * Reader for a {@link InfinispanInputSplit}
 *
 * @param <K> KeyType
 * @param <V> ValueType
 * @author gustavonalle
 * @since 0.1
 */
public class InfinispanRecordReader<K, V> extends RecordReader<K, V> {

   private CloseableIterator<Entry<Object, Object>> entryIterator;
   private K currentKey;
   private V currentValue;
   private RemoteCache<K, V> remoteCache;
   private final String filterFactory;
   private final Integer readBatchSize;

   public InfinispanRecordReader(RemoteCache<K, V> remoteCache, String filterFactory, int readBatchSize) {
      this.remoteCache = remoteCache;
      this.filterFactory = filterFactory;
      this.readBatchSize = readBatchSize;
   }

   @Override
   public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
      InfinispanInputSplit infinispanInputSplit = (InfinispanInputSplit) split;
      entryIterator = remoteCache.retrieveEntries(filterFactory, infinispanInputSplit.getSegments(), readBatchSize);
   }

   @Override
   public boolean nextKeyValue() throws IOException, InterruptedException {
      if (!entryIterator.hasNext()) {
         return false;
      }
      Entry<Object, Object> nextEntry = entryIterator.next();
      currentKey = (K) nextEntry.getKey();
      currentValue = (V) nextEntry.getValue();
      return true;
   }

   @Override
   public K getCurrentKey() throws IOException, InterruptedException {
      return currentKey;
   }

   @Override
   public V getCurrentValue() throws IOException, InterruptedException {
      return currentValue;
   }

   @Override
   public float getProgress() throws IOException, InterruptedException {
      return entryIterator.hasNext() ? 0 : 1;
   }

   @Override
   public void close() throws IOException {
      entryIterator.close();
      remoteCache.getRemoteCacheManager().stop();
   }

}
