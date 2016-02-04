package org.infinispan.hadoop.testutils.mapreduce;

import org.apache.hadoop.mapreduce.Reducer;
import org.infinispan.hadoop.testutils.domain.CategoryStats;

import java.io.IOException;


/**
 * Reducer using only externalizable objects
 */
public class InfinispanReducer extends Reducer<String, Integer, String, CategoryStats> {
   @Override
   protected void reduce(String key, Iterable<Integer> values, Context context) throws IOException, InterruptedException {
      int sum = 0;
      for (Integer val : values) {
         sum += val;
      }
      context.write(key, new CategoryStats(sum));
   }
}