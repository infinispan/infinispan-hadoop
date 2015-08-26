package org.infinispan.hadoop.sample;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.infinispan.hadoop.KeyValueConverter;

/***
 * Simple KeyValueConverter to be used with Reducers that operate on Writables, allowing to write simple primitives to
 * the cache
 *
 * @author Pedro Ruivo
 * @author gustavonalle
 */
public class OutputConverter implements KeyValueConverter<Text, IntWritable, String, Integer> {
   @Override
   public String convertKey(Text key) {
      return key.toString();
   }

   @Override
   public Integer convertValue(IntWritable value) {
      return value.get();
   }
}
