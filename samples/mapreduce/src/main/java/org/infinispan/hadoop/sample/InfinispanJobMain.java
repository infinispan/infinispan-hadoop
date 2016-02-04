package org.infinispan.hadoop.sample;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.infinispan.hadoop.InfinispanConfiguration;
import org.infinispan.hadoop.InfinispanInputFormat;
import org.infinispan.hadoop.InfinispanOutputFormat;

/**
 * Job reading and writing to Infinispan with Converters, reusing Map and Reducer classes that operates on Hadoop
 * Writables
 */
public class InfinispanJobMain {

   public static void main(String[] args) throws Exception {
      Configuration configuration = new Configuration();
      if (args.length < 1) {
         System.err.println("Usage: hadoop jar <job jar> InfinispanJobMain <ispn-server>");
         System.exit(2);
      }
      String host = args[0];

      configuration.set(InfinispanConfiguration.INPUT_REMOTE_CACHE_SERVER_LIST, host);

      configuration.set(InfinispanConfiguration.INPUT_REMOTE_CACHE_NAME, "map-reduce-in");
      configuration.set(InfinispanConfiguration.OUTPUT_REMOTE_CACHE_NAME, "map-reduce-out");

      configuration.set(InfinispanConfiguration.INPUT_KEY_VALUE_CONVERTER, InputConverter.class.getName());
      configuration.set(InfinispanConfiguration.OUTPUT_KEY_VALUE_CONVERTER, OutputConverter.class.getName());

      Job job = Job.getInstance(configuration, "Infinispan word count");
      job.setJarByClass(InfinispanJobMain.class);

      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);

      job.setMapperClass(MapClass.class);
      job.setReducerClass(ReduceClass.class);

      job.setInputFormatClass(InfinispanInputFormat.class);
      job.setOutputFormatClass(InfinispanOutputFormat.class);

      System.out.println("About to run the job!");

      boolean status = job.waitForCompletion(true);

      System.out.println("Finished executing job.");

      System.exit(status ? 0 : 1);
   }

}
