package org.infinispan.hadoop.testutils.converters;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.infinispan.hadoop.KeyValueConverter;
import org.infinispan.hadoop.testutils.domain.WebPage;

public class ToWritableInputConverter implements KeyValueConverter<Integer, WebPage, LongWritable, Text> {

   @Override
   public LongWritable convertKey(Integer key) {
      return new LongWritable((long) key);
   }

   @Override
   public Text convertValue(WebPage value) {
      return new Text(value.getAddress().toString() + "|" + value.getCategory());
   }
}