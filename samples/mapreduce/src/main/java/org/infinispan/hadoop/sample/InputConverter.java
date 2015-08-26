package org.infinispan.hadoop.sample;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.infinispan.hadoop.KeyValueConverter;

/***
 * Simple KeyValueConverter used to adapt mapper that operates on Writables
 *
 * @author Pedro Ruivo
 * @author gustavonalle
 */
public class InputConverter implements KeyValueConverter<Integer, String, LongWritable, Text> {

   @Override
   public LongWritable convertKey(Integer key) {
      return new LongWritable((long) key);
   }

   @Override
   public Text convertValue(String value) {
      return new Text(value);
   }

}
