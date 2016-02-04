package org.infinispan.hadoop.testutils.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.infinispan.hadoop.testutils.domain.CategoryStats;

import java.io.IOException;

/**
 * Reducer using mixed externalizable/writable objects
 */
public class CacheReducer extends Reducer<Text, IntWritable, String, CategoryStats> {

   @Override
   protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
         sum += val.get();
      }
      context.write(key.toString(), new CategoryStats(sum));
   }
}