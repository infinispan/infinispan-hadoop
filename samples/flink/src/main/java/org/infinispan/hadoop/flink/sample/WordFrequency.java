package org.infinispan.hadoop.flink.sample;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.infinispan.hadoop.InfinispanConfiguration;
import org.infinispan.hadoop.InfinispanInputFormat;

import java.util.List;
import java.util.stream.IntStream;

/**
 * Apache Flink job to calculate histograms with number of words on phrases.
 * <p>
 * This demonstrates connecting Flink with Infinispan by using {@link org.infinispan.hadoop.InfinispanInputFormat} and
 * {@link org.infinispan.hadoop.InfinispanOutputFormat}.
 * <p>Data is located in the cache "phrases" of Infinispan server with types &lt;Long,String&gt; and each entry has a
 * phrase with 1 or more words <br>
 * Output should be: <p>
 * HISTOGRAM:
 * <p>
 * 2 word phrases: ****** (6)<br>
 * 3 word phrases: ************ (12)<br>
 * 4 word phrases: ***************************************** (41)<br>
 * 5 word phrases: ********** (10)<br>
 * </p>
 */
public class WordFrequency {

   public static void main(String[] args) throws Exception {
      if (args.length < 1) {
         System.err.println("Usage: WordFrequency <ispn-server-ip>");
         System.exit(1);
      }

      // Configure the Infinispan InputFormat wrapping it in a Hadoop Job class.
      // In this sample, no writing to Infinispan will happen, so no need to configure
      // an InfinispanOutputFormat
      Configuration configuration = new Configuration();
      configuration.set(InfinispanConfiguration.INPUT_REMOTE_CACHE_HOST, args[0]);
      configuration.set(InfinispanConfiguration.INPUT_REMOTE_CACHE_NAME, "phrases");
      Job job = Job.getInstance(configuration, "Infinispan Integration");
      InfinispanInputFormat<Long, String> infinispanInputFormat = new InfinispanInputFormat<>();

      // Obtain the Execution environment from Flink
      final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

      // Create a DataSource that reads data using the InfinispanInputFormat
      DataSource<Tuple2<Long, String>> infinispanDS = env.createHadoopInput(infinispanInputFormat, Long.class, String.class, job);

      // Count entries
      long count = infinispanDS.count();

      // Obtain the values from entries
      DataSet<String> values = infinispanDS.map(entry -> entry.f1).returns(String.class);

      // Obtain phrase lengths (length,1) for the values
      DataSet<Tuple2<Integer, Integer>> lengthsCount = values.flatMap(new FlatMapFunction<String, Tuple2<Integer, Integer>>() {
         @Override
         public void flatMap(String s, Collector<Tuple2<Integer, Integer>> collector) throws Exception {
            collector.collect(new Tuple2<>(s.split(" ").length, 1));
         }
      });

      // Create the histogram
      List<Tuple2<Integer, Integer>> results = lengthsCount.groupBy(0).sum(1).collect();

      // Format results and print to stdout
      printResults(count, results);
   }

   private static void printResults(long entries, List<Tuple2<Integer, Integer>> results) {
      System.out.printf("TOTAL PHRASES ANALYZED: %d. HISTOGRAM:\n", entries);
      results.forEach(t -> {
         Integer wordNumber = t.f0;
         Integer count = t.f1;
         System.out.printf("%-3d word phrases:", wordNumber);
         IntStream.range(1, count).boxed().forEach(c -> System.out.print("*"));
         System.out.printf("(%d)\n", count);
      });
   }

}
