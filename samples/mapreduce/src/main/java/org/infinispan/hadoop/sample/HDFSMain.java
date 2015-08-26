package org.infinispan.hadoop.sample;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Word count job reading and writing to HDFS
 */
public class HDFSMain {

   public static void main(String[] args) throws Exception {
      Configuration conf = new Configuration();
      Job job = Job.getInstance(conf, "HDFS word count");
      job.setJarByClass(HDFSMain.class);
      job.setMapperClass(MapClass.class);
      job.setReducerClass(ReduceClass.class);
      job.setCombinerClass(ReduceClass.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);

      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));

      boolean status = job.waitForCompletion(true);
      System.exit(status ? 0 : 1);
   }
}
