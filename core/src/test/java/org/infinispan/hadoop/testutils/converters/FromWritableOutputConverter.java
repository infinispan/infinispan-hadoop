package org.infinispan.hadoop.testutils.converters;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.infinispan.hadoop.KeyValueConverter;
import org.infinispan.hadoop.testutils.domain.CategoryStats;

public class FromWritableOutputConverter implements KeyValueConverter<Text, IntWritable, String, CategoryStats> {
   @Override
   public String convertKey(Text key) {
      return key.toString();
   }

   @Override
   public CategoryStats convertValue(IntWritable value) {
      return new CategoryStats(value.get());
   }
}