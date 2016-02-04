package org.infinispan.hadoop.testutils.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.infinispan.hadoop.testutils.domain.WebPage;

import java.io.IOException;

/**
 * Mapper using mixed externalizable/writable objects
 */
public class CacheMapper extends Mapper<Integer, WebPage, Text, IntWritable> {
   private final static IntWritable one = new IntWritable(1);
   private Text word = new Text();

   @Override
   protected void map(Integer key, WebPage webPage, Context context) throws IOException, InterruptedException {
      word.set(webPage.getCategory());
      context.write(word, one);
   }
}