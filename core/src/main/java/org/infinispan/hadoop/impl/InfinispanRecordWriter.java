package org.infinispan.hadoop.impl;

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.infinispan.client.hotrod.RemoteCache;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Write output to the configured cache.
 *
 * @param <K> Key type
 * @param <V> Value type
 * @author gustavonalle
 * @since 0.1
 */
public class InfinispanRecordWriter<K, V> extends RecordWriter<K, V> {
   private final RemoteCache<K, V> remoteCache;
   private final int writeBatchSize;
   private final Map<K, V> batch;

   public InfinispanRecordWriter(RemoteCache<K, V> remoteCache, int writeBatchSize) {
      this.remoteCache = remoteCache;
      this.writeBatchSize = writeBatchSize;
      this.batch = new HashMap<>(writeBatchSize);
   }

   @Override
   public void write(K key, V value) throws IOException, InterruptedException {
      batch.put(key, value);
      if (batch.size() >= writeBatchSize) {
         remoteCache.putAll(batch);
         batch.clear();
      }
   }

   @Override
   public void close(TaskAttemptContext context) throws IOException, InterruptedException {
      if (!batch.isEmpty()) {
         remoteCache.putAll(batch);
      }
   }
}
