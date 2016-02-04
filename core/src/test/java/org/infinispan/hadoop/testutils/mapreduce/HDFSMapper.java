package org.infinispan.hadoop.testutils.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper using Hadoop writables only
 */
public class HDFSMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
   private final static IntWritable one = new IntWritable(1);
   private Text word = new Text();

   @Override
   protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String category = value.toString().split("\\|")[1];
      word.set(category);
      context.write(word, one);
   }
}
