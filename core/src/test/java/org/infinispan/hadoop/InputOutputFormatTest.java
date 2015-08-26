package org.infinispan.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.client.hotrod.CacheTopologyInfo;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.hadoop.impl.InfinispanInputSplit;
import org.infinispan.hadoop.testutils.domain.CategoryStats;
import org.infinispan.hadoop.testutils.hadoop.MiniHadoopCluster;
import org.infinispan.filter.AbstractKeyValueFilterConverter;
import org.infinispan.filter.KeyValueFilterConverter;
import org.infinispan.filter.KeyValueFilterConverterFactory;
import org.infinispan.filter.NamedFactory;
import org.infinispan.hadoop.testutils.domain.WebPage;
import org.infinispan.metadata.Metadata;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.container.test.api.OverProtocol;
import org.jboss.arquillian.container.test.api.TargetsContainer;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.SocketAddress;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertTrue;

@RunWith(Arquillian.class)
public class InputOutputFormatTest {

   private static final String INPUT_CACHE_NAME = "default";
   private static final String OUTPUT_CACHE_NAME = "namedCache";
   public static final String GOVERNMENT_PAGE_FILTER_FACTORY = "GovernmentPageFilterFactory";

   @InfinispanResource("container1")
   RemoteInfinispanServer server1;

   @InfinispanResource("container2")
   RemoteInfinispanServer server2;

   @Deployment(testable = false, name = "server-filter-1")
   @TargetsContainer("container1")
   @OverProtocol("jmx-as7")
   public static Archive<?> deploy1() throws IOException {
      return createFilterArchive();
   }

   @Deployment(testable = false, name = "server-filter-2")
   @TargetsContainer("container2")
   @OverProtocol("jmx-as7")
   public static Archive<?> deploy2() throws IOException {
      return createFilterArchive();
   }

   private MiniHadoopCluster miniHadoopCluster = new MiniHadoopCluster();

   private RemoteCache<Integer, WebPage> inputCache;
   private RemoteCache<String, CategoryStats> outputCache;

   @Before
   public void prepare() throws Exception {
      miniHadoopCluster.start();
      Configuration configuration = miniHadoopCluster.getConfiguration();

      String host1Address = server1.getHotrodEndpoint().getInetAddress().getHostAddress();
      int port1 = server1.getHotrodEndpoint().getPort();

      String host2Address = server2.getHotrodEndpoint().getInetAddress().getHostAddress();
      int port2 = server2.getHotrodEndpoint().getPort();

      configuration.set(InfinispanConfiguration.INPUT_REMOTE_CACHE_HOST, host1Address);
      configuration.setInt(InfinispanConfiguration.INPUT_REMOTE_CACHE_PORT, port1);
      configuration.set(InfinispanConfiguration.INPUT_REMOTE_CACHE_NAME, INPUT_CACHE_NAME);

      configuration.set(InfinispanConfiguration.OUTPUT_REMOTE_CACHE_NAME, OUTPUT_CACHE_NAME);
      configuration.set(InfinispanConfiguration.OUTPUT_REMOTE_CACHE_HOST, host2Address);
      configuration.setInt(InfinispanConfiguration.OUTPUT_REMOTE_CACHE_PORT, port2);

      configuration.set("io.serializations", "org.apache.hadoop.io.serializer.WritableSerialization, org.infinispan.hadoop.serialization.JBossMarshallerSerialization");
      configuration.set(InfinispanConfiguration.SERIALIZATION_CLASSES, "java.lang.String, java.lang.Integer," + WebPage.class.getName() + "," + CategoryStats.class.getName());

      inputCache = new RemoteCacheManager(
              new ConfigurationBuilder().addServer().host(host1Address).port(port1).build()).getCache(INPUT_CACHE_NAME);

      outputCache = new RemoteCacheManager(
              new ConfigurationBuilder().addServer().host(host2Address).port(port2).build()).getCache(OUTPUT_CACHE_NAME);

      inputCache.clear();
      outputCache.clear();

      List<WebPage> webPages = createData();

      saveToInputCache(webPages);
      saveToHDFS(webPages);
   }


   private List<WebPage> createData() throws MalformedURLException {
      List<WebPage> webPages = new ArrayList<>(8);
      webPages.add(new WebPage(new URL("http://www.jboss.org"), "software", 1000L));
      webPages.add(new WebPage(new URL("http://www.netflix.com"), "streaming", 100L));
      webPages.add(new WebPage(new URL("http://www.cia.gov"), "government", 1000L));
      webPages.add(new WebPage(new URL("http://www.gov.uk"), "government", 1L));
      webPages.add(new WebPage(new URL("http://www.fbi.gov"), "government", 1000L));
      webPages.add(new WebPage(new URL("http://www.scala-lang.org"), "software", 1000L));
      return webPages;
   }

   @After
   public void stop() throws Exception {
      miniHadoopCluster.shutDown();
   }

   @Test
   public void testReadAndWriteFromHDFS() throws Exception {
      Job job = Job.getInstance(miniHadoopCluster.getConfiguration());

      FileInputFormat.addInputPath(job, new Path("/input"));
      FileOutputFormat.setOutputPath(job, new Path("/output"));

      job.setMapperClass(HDFSMapper.class);
      job.setReducerClass(HDFSReducer.class);

      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);
      job.setNumReduceTasks(1);

      job.waitForCompletion(true);

      Map<String, Integer> resultMap = readResultFromHDFS();
      assertTrue(1 == resultMap.get("streaming"));
      assertTrue(2 == resultMap.get("software"));
      assertTrue(3 == resultMap.get("government"));
   }

   @Test
   public void testReuseExistingJobWithInfinispan() throws Exception {
      Configuration configuration = miniHadoopCluster.getConfiguration();

      configuration.set(InfinispanConfiguration.INPUT_KEY_VALUE_CONVERTER, ToWritableInputConverter.class.getName());
      configuration.set(InfinispanConfiguration.OUTPUT_KEY_VALUE_CONVERTER, FromWritableOutputConverter.class.getName());

      Job job = Job.getInstance(configuration);
      job.setInputFormatClass(InfinispanInputFormat.class);
      job.setOutputFormatClass(InfinispanOutputFormat.class);

      job.setMapperClass(HDFSMapper.class);
      job.setReducerClass(HDFSReducer.class);

      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);
      job.setNumReduceTasks(1);

      job.waitForCompletion(true);

      assertTrue(1 == outputCache.get("streaming").getCount());
      assertTrue(2 == outputCache.get("software").getCount());
      assertTrue(3 == outputCache.get("government").getCount());
   }

   @Test
   public void testReadFromInfinispanSaveToHDFS() throws Exception {
      Configuration configuration = miniHadoopCluster.getConfiguration();
      configuration.set(InfinispanConfiguration.OUTPUT_KEY_VALUE_CONVERTER, ToWritableOutputConverter.class.getName());

      Job job = Job.getInstance(configuration);

      job.setMapperClass(CacheMapper.class);
      job.setReducerClass(HDFSReducer.class);

      job.setInputFormatClass(InfinispanInputFormat.class);

      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);
      job.setNumReduceTasks(1);

      FileOutputFormat.setOutputPath(job, new Path("/output"));

      job.waitForCompletion(true);

      Map<String, Integer> resultMap = readResultFromHDFS();
      assertTrue(1 == resultMap.get("streaming"));
      assertTrue(2 == resultMap.get("software"));
      assertTrue(3 == resultMap.get("government"));
   }

   @Test
   public void testReadFromHDFSSaveToInfinispan() throws Exception {
      Configuration configuration = miniHadoopCluster.getConfiguration();
      configuration.set(JobContext.KEY_COMPARATOR, Text.Comparator.class.getName());

      Job job = Job.getInstance(configuration);

      // Input Config
      FileInputFormat.addInputPath(job, new Path("/input"));
      job.setMapperClass(HDFSMapper.class);
      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(IntWritable.class);

      // OutputConfig
      job.setReducerClass(CacheReducer.class);
      job.setOutputFormatClass(InfinispanOutputFormat.class);
      job.setOutputKeyClass(String.class);
      job.setOutputValueClass(CategoryStats.class);

      job.waitForCompletion(true);

      assertTrue(1 == outputCache.get("streaming").getCount());
      assertTrue(2 == outputCache.get("software").getCount());
      assertTrue(3 == outputCache.get("government").getCount());
   }

   @Test
   public void testReadAndWriteToInfinispan() throws Exception {
      Configuration configuration = miniHadoopCluster.getConfiguration();
      configuration.set(JobContext.KEY_COMPARATOR, Text.Comparator.class.getName());

      Job job = Job.getInstance(configuration);

      job.setInputFormatClass(InfinispanInputFormat.class);
      job.setOutputFormatClass(InfinispanOutputFormat.class);

      job.setMapperClass(InfinispanMapper.class);
      job.setMapOutputValueClass(Integer.class);
      job.setReducerClass(InfinispanReducer.class);

      job.setOutputKeyClass(String.class);
      job.setOutputValueClass(CategoryStats.class);

      job.waitForCompletion(true);

      assertTrue(1 == outputCache.get("streaming").getCount());
      assertTrue(2 == outputCache.get("software").getCount());
      assertTrue(3 == outputCache.get("government").getCount());
   }

   @Test
   public void testReadAndWriteToInfinispanWithFilter() throws Exception {
      Configuration configuration = miniHadoopCluster.getConfiguration();
      configuration.set(InfinispanConfiguration.INPUT_FILTER_FACTORY, GOVERNMENT_PAGE_FILTER_FACTORY);
      configuration.set(JobContext.KEY_COMPARATOR, Text.Comparator.class.getName());

      Job job = Job.getInstance(configuration);

      job.setInputFormatClass(InfinispanInputFormat.class);
      job.setOutputFormatClass(InfinispanOutputFormat.class);

      job.setMapperClass(CacheMapper.class);
      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(IntWritable.class);
      job.setReducerClass(CacheReducer.class);

      job.setOutputKeyClass(String.class);
      job.setOutputValueClass(CategoryStats.class);

      job.waitForCompletion(true);

      assertTrue(1 == outputCache.size());
      assertTrue(3 == outputCache.get("government").getCount());
   }

   @Test
   public void testCustomSplitter() throws Exception {
      Configuration configuration = miniHadoopCluster.getConfiguration();
      configuration.set(InfinispanConfiguration.SPLITTER_CLASS, CustomSplitter.class.getName());
      configuration.set(JobContext.KEY_COMPARATOR, Text.Comparator.class.getName());

      Job job = Job.getInstance(configuration);

      job.setInputFormatClass(InfinispanInputFormat.class);
      job.setOutputFormatClass(InfinispanOutputFormat.class);

      job.setMapperClass(InfinispanMapper.class);
      job.setMapOutputValueClass(Integer.class);
      job.setReducerClass(InfinispanReducer.class);

      job.setOutputKeyClass(String.class);
      job.setOutputValueClass(CategoryStats.class);

      job.waitForCompletion(true);

      assertTrue(1 == outputCache.get("streaming").getCount());
      assertTrue(2 == outputCache.get("software").getCount());
      assertTrue(3 == outputCache.get("government").getCount());
   }

   private void saveToHDFS(List<WebPage> webPages) throws IOException {
      FileSystem fileSystem = miniHadoopCluster.getFileSystem();
      OutputStream os = fileSystem.create(new Path("/input"));
      BufferedWriter br = new BufferedWriter(new OutputStreamWriter(os, "UTF-8"));
      for (WebPage webPage : webPages) {
         br.write(webPage.getAddress() + "|" + webPage.getCategory() + "\n");
      }
      br.close();
   }

   private void saveToInputCache(List<WebPage> webPages) {
      for (int i = 0; i < webPages.size(); i++) {
         inputCache.put(i + 1, webPages.get(i));
      }
   }

   private Map<String, Integer> readResultFromHDFS() throws IOException {
      FileSystem fileSystem = miniHadoopCluster.getFileSystem();

      Map<String, Integer> map = new HashMap<>();
      Path path = new Path("/output/part-r-00000");
      FSDataInputStream fsDataInputStream = fileSystem.open(path);
      BufferedReader br = new BufferedReader(new InputStreamReader(fsDataInputStream));
      String line;
      while ((line = br.readLine()) != null) {
         String[] wordCount = line.split("\t");
         map.put(wordCount[0], Integer.parseInt(wordCount[1]));
      }
      br.close();
      return map;
   }

   /**
    * Custom splitter implementation that generates 2 splits per server
    */
   public static class CustomSplitter implements InfinispanSplitter {

      @Override
      public List<InputSplit> calculateSplits(CacheTopologyInfo cacheTopologyInfo) throws IOException {
         Map<SocketAddress, Set<Integer>> segmentsPerServer = cacheTopologyInfo.getSegmentsPerServer();
         if (segmentsPerServer.isEmpty()) {
            throw new IOException("No servers to split");
         }
         List<InputSplit> inputSplits = new ArrayList<>();
         Set<Integer> taken = new HashSet<>();

         for (Map.Entry<SocketAddress, Set<Integer>> server : segmentsPerServer.entrySet()) {
            Set<Integer> segments = server.getValue();
            segments.removeAll(taken);
            Integer[] objects = segments.toArray(new Integer[segments.size()]);
            int segmentsSize = segments.size();
            Integer[] firstPart = Arrays.copyOfRange(objects, 0, segmentsSize / 2);
            Integer[] lastPart = Arrays.copyOfRange(objects, segmentsSize / 2, segmentsSize);

            List<Integer> initChunk = Arrays.asList(firstPart);
            List<Integer> endChunk = Arrays.asList(lastPart);
            Set<Integer> s1 = new HashSet<>();
            Set<Integer> s2 = new HashSet<>();
            s1.addAll(initChunk);
            s2.addAll(endChunk);
            inputSplits.add(new InfinispanInputSplit(s1, ((InetSocketAddress) server.getKey()).getHostName()));
            inputSplits.add(new InfinispanInputSplit(s2, ((InetSocketAddress) server.getKey()).getHostName()));

            taken.addAll(segments);
         }
         return inputSplits;
      }
   }

   /**
    * Mapper reducer pair using only externalizable objects
    */
   private static class InfinispanMapper extends Mapper<Integer, WebPage, String, Integer> {
      @Override
      protected void map(Integer key, WebPage webPage, Context context) throws IOException, InterruptedException {
         context.write(webPage.getCategory(), 1);
      }
   }

   private static class InfinispanReducer extends Reducer<String, Integer, String, CategoryStats> {
      @Override
      protected void reduce(String key, Iterable<Integer> values, Context context) throws IOException, InterruptedException {
         int sum = 0;
         for (Integer val : values) {
            sum += val;
         }
         context.write(key, new CategoryStats(sum));
      }
   }

   /**
    * Mapper reducer pair using mixed externalizable/writable objects
    */
   private static class CacheMapper extends Mapper<Integer, WebPage, Text, IntWritable> {
      private final static IntWritable one = new IntWritable(1);
      private Text word = new Text();

      @Override
      protected void map(Integer key, WebPage webPage, Context context) throws IOException, InterruptedException {
         word.set(webPage.getCategory());
         context.write(word, one);
      }
   }

   private static class CacheReducer extends Reducer<Text, IntWritable, String, CategoryStats> {

      @Override
      protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
         int sum = 0;
         for (IntWritable val : values) {
            sum += val.get();
         }
         context.write(key.toString(), new CategoryStats(sum));
      }
   }

   /**
    * Mapper reducer pair using Hadoop writables only
    */
   private static class HDFSMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
      private final static IntWritable one = new IntWritable(1);
      private Text word = new Text();

      @Override
      protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
         String category = value.toString().split("\\|")[1];
         word.set(category);
         context.write(word, one);
      }
   }

   private static class HDFSReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
      private IntWritable result = new IntWritable();

      public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
         int sum = 0;
         for (IntWritable val : values) {
            sum += val.get();
         }
         result.set(sum);
         context.write(key, result);
      }
   }

   /**
    * Converters
    */
   public static class ToWritableInputConverter implements KeyValueConverter<Integer, WebPage, LongWritable, Text> {

      @Override
      public LongWritable convertKey(Integer key) {
         return new LongWritable((long) key);
      }

      @Override
      public Text convertValue(WebPage value) {
         return new Text(value.getAddress().toString() + "|" + value.getCategory());
      }
   }

   public static class ToWritableOutputConverter implements KeyValueConverter<Text, IntWritable, String, CategoryStats> {
      @Override
      public String convertKey(Text key) {
         return key.toString();
      }

      @Override
      public CategoryStats convertValue(IntWritable value) {
         return new CategoryStats(value.get());
      }
   }

   public static class FromWritableOutputConverter implements KeyValueConverter<Text, IntWritable, String, CategoryStats> {
      @Override
      public String convertKey(Text key) {
         return key.toString();
      }

      @Override
      public CategoryStats convertValue(IntWritable value) {
         return new CategoryStats(value.get());
      }
   }

   private static Archive<?> createFilterArchive() throws IOException {
      return ShrinkWrap.create(JavaArchive.class, "server-filter.jar")
              .addClasses(CustomFilterFactory.class, CustomFilterFactory.GovernmentFilter.class, WebPage.class)
              .addAsServiceProvider(KeyValueFilterConverterFactory.class, CustomFilterFactory.class);
   }

   @NamedFactory(name = GOVERNMENT_PAGE_FILTER_FACTORY)
   public static class CustomFilterFactory implements KeyValueFilterConverterFactory<Integer, WebPage, WebPage> {
      @Override
      public KeyValueFilterConverter<Integer, WebPage, WebPage> getFilterConverter() {
         return new GovernmentFilter();
      }

      public static class GovernmentFilter extends AbstractKeyValueFilterConverter<Integer, WebPage, WebPage> implements Serializable {
         @Override
         public WebPage filterAndConvert(Integer key, WebPage value, Metadata metadata) {
            String host = value.getAddress().getHost();
            return host.endsWith("gov") || host.endsWith("gov.uk") ? value : null;
         }
      }
   }


}
