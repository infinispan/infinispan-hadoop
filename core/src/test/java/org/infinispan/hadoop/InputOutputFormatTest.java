package org.infinispan.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.infinispan.arquillian.core.HotRodEndpoint;
import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.filter.KeyValueFilterConverterFactory;
import org.infinispan.hadoop.impl.InfinispanInputSplit;
import org.infinispan.hadoop.testutils.converters.CustomFilterFactory;
import org.infinispan.hadoop.testutils.converters.FromWritableOutputConverter;
import org.infinispan.hadoop.testutils.converters.ToWritableInputConverter;
import org.infinispan.hadoop.testutils.converters.ToWritableOutputConverter;
import org.infinispan.hadoop.testutils.domain.CategoryStats;
import org.infinispan.hadoop.testutils.domain.SimpleDomain;
import org.infinispan.hadoop.testutils.domain.WebPage;
import org.infinispan.hadoop.testutils.hadoop.MiniHadoopCluster;
import org.infinispan.hadoop.testutils.mapreduce.CacheMapper;
import org.infinispan.hadoop.testutils.mapreduce.CacheReducer;
import org.infinispan.hadoop.testutils.mapreduce.GeneralCacheMapper;
import org.infinispan.hadoop.testutils.mapreduce.GeneralInfinispanMapper;
import org.infinispan.hadoop.testutils.mapreduce.HDFSMapper;
import org.infinispan.hadoop.testutils.mapreduce.HDFSReducer;
import org.infinispan.hadoop.testutils.mapreduce.InfinispanMapper;
import org.infinispan.hadoop.testutils.mapreduce.InfinispanReducer;
import org.jboss.arquillian.container.test.api.ContainerController;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.container.test.api.OverProtocol;
import org.jboss.arquillian.container.test.api.TargetsContainer;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.arquillian.test.api.ArquillianResource;
import org.jboss.shrinkwrap.api.Archive;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.infinispan.hadoop.testutils.Utils.readResultFromHDFS;
import static org.infinispan.hadoop.testutils.Utils.saveToHDFS;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(Arquillian.class)
public class InputOutputFormatTest {

   private static final String INPUT_CACHE_NAME = "default";
   private static final String OUTPUT_CACHE_NAME = "memcachedCache";
   public static final String GOVERNMENT_PAGE_FILTER_FACTORY = "GovernmentPageFilterFactory";

   @InfinispanResource("container1")
   RemoteInfinispanServer server1;

   @InfinispanResource("container2")
   RemoteInfinispanServer server2;

   @ArquillianResource
   ContainerController controller;

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

   private static MiniHadoopCluster miniHadoopCluster = new MiniHadoopCluster();

   private RemoteCache inputCache;
   private RemoteCache<String, CategoryStats> outputCache;
   private static int testCounter = 0;
   private static int numberOfTestMethods = 0;

   private String buildServerList(RemoteInfinispanServer... servers) {
      StringBuilder stringBuilder = new StringBuilder();
      for (int i = 0; i < servers.length; i++) {
         HotRodEndpoint hotrodEndpoint = servers[i].getHotrodEndpoint();
         stringBuilder.append(hotrodEndpoint.getInetAddress().getHostName()).append(":").append(hotrodEndpoint.getPort());
         if (i < servers.length - 1) {
            stringBuilder.append(";");
         }
      }
      return stringBuilder.toString();
   }

   @BeforeClass
   public static void initHadoopAndServers() throws IOException {
      miniHadoopCluster.start();

      for (Method method : InputOutputFormatTest.class.getMethods()) {
         if (method.isAnnotationPresent(Test.class)) {
            numberOfTestMethods++;
         }
      }
   }

   @AfterClass
   public static void destroyHadoop() throws IOException {
      miniHadoopCluster.shutDown();
   }

   private Configuration createBaseConfiguration() {
      Configuration configuration = new YarnConfiguration();
      Configuration baseConfiguration = miniHadoopCluster.getConfiguration();
      baseConfiguration.iterator().forEachRemaining(c -> configuration.set(c.getKey(), c.getValue()));

      String serverList = buildServerList(server1, server2);
      configuration.set(InfinispanConfiguration.INPUT_REMOTE_CACHE_SERVER_LIST, serverList);
      configuration.set(InfinispanConfiguration.INPUT_REMOTE_CACHE_NAME, INPUT_CACHE_NAME);

      configuration.set(InfinispanConfiguration.OUTPUT_REMOTE_CACHE_NAME, OUTPUT_CACHE_NAME);
      configuration.set(InfinispanConfiguration.OUTPUT_REMOTE_CACHE_SERVER_LIST, serverList);

      configuration.set("io.serializations", "org.apache.hadoop.io.serializer.WritableSerialization, org.infinispan.hadoop.serialization.JBossMarshallerSerialization");
      configuration.set(InfinispanConfiguration.SERIALIZATION_CLASSES, "java.lang.String, java.lang.Integer," + WebPage.class.getName() + "," + CategoryStats.class.getName());

      return configuration;
   }

   @Before
   public void prepare() throws Exception {
      //If already started don't start.
      if (!controller.isStarted("container1")) {
         controller.start("container1");
      }

      if (!controller.isStarted("container2")) {
         controller.start("container2");
      }
      testCounter++;

      String host1Address = server1.getHotrodEndpoint().getInetAddress().getHostAddress();
      int port1 = server1.getHotrodEndpoint().getPort();

      String host2Address = server2.getHotrodEndpoint().getInetAddress().getHostAddress();
      int port2 = server2.getHotrodEndpoint().getPort();

      inputCache = new RemoteCacheManager(
              new ConfigurationBuilder().addServer().host(host1Address).port(port1).build()).getCache(INPUT_CACHE_NAME);

      outputCache = new RemoteCacheManager(
              new ConfigurationBuilder().addServer().host(host2Address).port(port2).build()).getCache(OUTPUT_CACHE_NAME);

      inputCache.clear();
      outputCache.clear();

      List<WebPage> webPages = createData();

      saveToInputCache(webPages);
      saveToHDFS(miniHadoopCluster, webPages);
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
      inputCache.getRemoteCacheManager().stop();
      outputCache.getRemoteCacheManager().stop();

      //If the last test is executed stop servers.
      if (testCounter == numberOfTestMethods) {
         if (controller.isStarted("container1")) {
            controller.stop("container1");
         }

         if (controller.isStarted("container2")) {
            controller.stop("container2");
         }
      }
   }

   @Test
   public void testReadAndWriteFromHDFS() throws Exception {
      Job job = Job.getInstance(createBaseConfiguration());

      FileInputFormat.addInputPath(job, new Path("/input"));

      String outputPath = "/output-testReadAndWriteFromHDFS";
      FileOutputFormat.setOutputPath(job, new Path(outputPath));

      job.setMapperClass(HDFSMapper.class);
      job.setReducerClass(HDFSReducer.class);

      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);
      job.setNumReduceTasks(1);

      job.waitForCompletion(true);

      Map<String, Integer> resultMap = readResultFromHDFS(miniHadoopCluster, outputPath);
      assertTrue(1 == resultMap.get("streaming"));
      assertTrue(2 == resultMap.get("software"));
      assertTrue(3 == resultMap.get("government"));
   }

   @Test
   public void testReuseExistingJobWithInfinispan() throws Exception {
      Configuration configuration = createBaseConfiguration();

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
      Configuration configuration = createBaseConfiguration();
      configuration.set(InfinispanConfiguration.OUTPUT_KEY_VALUE_CONVERTER, ToWritableOutputConverter.class.getName());

      Job job = Job.getInstance(configuration);

      job.setMapperClass(CacheMapper.class);
      job.setReducerClass(HDFSReducer.class);

      job.setInputFormatClass(InfinispanInputFormat.class);

      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);
      job.setNumReduceTasks(1);

      String outputPath = "/output-testReadFromInfinispanSaveToHDFS";
      FileOutputFormat.setOutputPath(job, new Path(outputPath));

      job.waitForCompletion(true);

      Map<String, Integer> resultMap = readResultFromHDFS(miniHadoopCluster, outputPath);
      assertTrue(1 == resultMap.get("streaming"));
      assertTrue(2 == resultMap.get("software"));
      assertTrue(3 == resultMap.get("government"));
   }

   /**
    * Verifies that if there are different types of objects in the cache, the implemented general mapper will perform mapping
    * for all of them.
    *
    * @throws Exception
     */
   @Test
   public void testReadFromInfinispanSaveToHDFS1() throws Exception {
      inputCache.put(1000, new SimpleDomain("streaming"));
      inputCache.put(1001, new SimpleDomain("government"));
      inputCache.put(1002, new SimpleDomain("software"));

      Configuration configuration = createBaseConfiguration();
      configuration.set(InfinispanConfiguration.OUTPUT_KEY_VALUE_CONVERTER, ToWritableOutputConverter.class.getName());

      Job job = Job.getInstance(configuration);

      job.setMapperClass(GeneralCacheMapper.class);
      job.setReducerClass(HDFSReducer.class);

      job.setInputFormatClass(InfinispanInputFormat.class);

      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);
      job.setNumReduceTasks(1);

      String outputPath = "/output-testReadFromInfinispanSaveToHDFS1";
      FileOutputFormat.setOutputPath(job, new Path(outputPath));

      job.waitForCompletion(true);

      Map<String, Integer> resultMap = readResultFromHDFS(miniHadoopCluster, outputPath);
      assertTrue(2 == resultMap.get("streaming"));
      assertTrue(3 == resultMap.get("software"));
      assertTrue(4 == resultMap.get("government"));
   }

   @Test
   public void testReadFromHDFSSaveToInfinispan() throws Exception {
      Configuration configuration = createBaseConfiguration();
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

   /**
    * Tests the HDFS -> Infinispan MapReduce task using {@link InfinispanConfiguration#INPUT_READ_BATCH_SIZE}
    * and {@link InfinispanConfiguration#OUTPUT_WRITE_BATCH_SIZE}
    * properties.
    * @throws Exception
     */
   @Test
   public void testReadFromHDFSSaveToInfinispanWithLowWriteBatchSize() throws Exception {
      Configuration configuration = createBaseConfiguration();
      configuration.set(JobContext.KEY_COMPARATOR, Text.Comparator.class.getName());
      configuration.set(InfinispanConfiguration.OUTPUT_WRITE_BATCH_SIZE, "2");

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
      Configuration configuration = createBaseConfiguration();
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

   /**
    * Verifies that when the cache contains objects of different types, the implemented general mapper will return
    * the mapreduce result for specified one/or for all of them.
    *
    * @throws Exception
     */
   @Test
   public void testReadAndWriteToInfinispan1() throws Exception {
      //Inserting into cache new SimpleDomain objects
      inputCache.put(1000, new SimpleDomain("streaming"));
      inputCache.put(1001, new SimpleDomain("government"));
      inputCache.put(1002, new SimpleDomain("software"));

      Configuration configuration = createBaseConfiguration();
      configuration.set(JobContext.KEY_COMPARATOR, Text.Comparator.class.getName());

      Job job = Job.getInstance(configuration);

      job.setInputFormatClass(InfinispanInputFormat.class);
      job.setOutputFormatClass(InfinispanOutputFormat.class);

      job.setMapperClass(GeneralInfinispanMapper.class);
      job.setMapOutputValueClass(Integer.class);

      job.setReducerClass(InfinispanReducer.class);
      job.setOutputKeyClass(String.class);
      job.setOutputValueClass(CategoryStats.class);

      job.waitForCompletion(true);

      assertTrue(2 == outputCache.get("streaming").getCount());
      assertTrue(3 == outputCache.get("software").getCount());
      assertTrue(4 == outputCache.get("government").getCount());
   }

   /**
    * Verifies that when the serializable type is not set into configuration using {@link InfinispanConfiguration#SERIALIZATION_CLASSES}
    * property, the result of mapreduce is empty.
    *
    * @throws Exception
     */
   @Test
   public void testReadAndWriteToInfinispan2() throws Exception {
      //Inserting into cache new SimpleDomain objects
      inputCache.put(1000, new SimpleDomain("streaming"));
      inputCache.put(1001, new SimpleDomain("government"));
      inputCache.put(1002, new SimpleDomain("software"));

      Configuration configuration = createBaseConfiguration();
      configuration.set(JobContext.KEY_COMPARATOR, Text.Comparator.class.getName());
      configuration.set(InfinispanConfiguration.SERIALIZATION_CLASSES, SimpleDomain.class.getName());
      Job job = Job.getInstance(configuration);

      job.setInputFormatClass(InfinispanInputFormat.class);
      job.setOutputFormatClass(InfinispanOutputFormat.class);

      job.setMapperClass(GeneralInfinispanMapper.class);
      job.setMapOutputValueClass(Integer.class);

      job.setReducerClass(InfinispanReducer.class);
      job.setOutputKeyClass(String.class);
      job.setOutputValueClass(CategoryStats.class);

      job.waitForCompletion(true);

      assertTrue(outputCache.isEmpty());
   }

   /**
    * Tests the use of {@link InfinispanConfiguration#INPUT_READ_BATCH_SIZE}
    * and {@link InfinispanConfiguration#OUTPUT_WRITE_BATCH_SIZE}
    * properties.
    *
    * @throws Exception
     */
   @Test
   public void testReadAndWriteToInfinispanWithLowReadAndWriteBatchSize() throws Exception {
      Configuration configuration = createBaseConfiguration();
      configuration.set(JobContext.KEY_COMPARATOR, Text.Comparator.class.getName());
      configuration.set(InfinispanConfiguration.INPUT_READ_BATCH_SIZE, "1");
      configuration.set(InfinispanConfiguration.OUTPUT_WRITE_BATCH_SIZE, "1");

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
      Configuration configuration = createBaseConfiguration();
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

   /**
    * Tests the Infinispan -> HDFS MapReduce execution with filtering of the input using
    * {@link InfinispanConfiguration#INPUT_FILTER_FACTORY} property.
    *
    * @throws Exception
     */
   @Test
   public void testReadFromInfinispanSaveToHDFSWithFilterFactory() throws Exception {
      Configuration configuration = createBaseConfiguration();
      configuration.set(InfinispanConfiguration.OUTPUT_KEY_VALUE_CONVERTER, ToWritableOutputConverter.class.getName());
      configuration.set(InfinispanConfiguration.INPUT_FILTER_FACTORY, GOVERNMENT_PAGE_FILTER_FACTORY);

      Job job = Job.getInstance(configuration);

      job.setMapperClass(CacheMapper.class);
      job.setReducerClass(HDFSReducer.class);

      job.setInputFormatClass(InfinispanInputFormat.class);

      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);
      job.setNumReduceTasks(1);

      String outputPath = "/output-testReadFromInfinispanSaveToHDFSWithFilterFactory";
      FileOutputFormat.setOutputPath(job, new Path(outputPath));

      job.waitForCompletion(true);

      Map<String, Integer> resultMap = readResultFromHDFS(miniHadoopCluster, outputPath);
      assertTrue(1 == resultMap.size());
      assertTrue(3 == resultMap.get("government"));
   }

   @Test
   public void testCustomSplitter() throws Exception {
      Configuration configuration = createBaseConfiguration();
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

   @Test
   public void testPreferredServerUnreachable() throws Exception {
      InfinispanInputSplit invalidSplit = createInfinispanSplit();

      Configuration configuration = miniHadoopCluster.getConfiguration();
      TaskAttemptContextImpl fakeTaskContext = new TaskAttemptContextImpl(configuration, new TaskAttemptID());
      InfinispanInputFormat<Integer, WebPage> inputFormat = new InfinispanInputFormat<>();
      RecordReader<Integer, WebPage> reader = inputFormat.createRecordReader(invalidSplit, fakeTaskContext);
      reader.initialize(invalidSplit, fakeTaskContext);

      reader.nextKeyValue();
      assertNotNull(reader.getCurrentKey());
   }

   private InfinispanInputSplit createInfinispanSplit() {
      int invalidPort = 3421;
      InetSocketAddress unreachable = InetSocketAddress.createUnresolved("localhost", invalidPort);
      return new InfinispanInputSplit(new HashSet<>(Arrays.asList(1, 2, 3)), unreachable);
   }

   private void saveToInputCache(List<WebPage> webPages) {
      for (int i = 0; i < webPages.size(); i++) {
         inputCache.put(i + 1, webPages.get(i));
      }
   }

   private static Archive<?> createFilterArchive() throws IOException {
      return ShrinkWrap.create(JavaArchive.class, "server-filter.jar")
              .addClasses(CustomFilterFactory.class, CustomFilterFactory.GovernmentFilter.class, WebPage.class)
              .addAsServiceProvider(KeyValueFilterConverterFactory.class, CustomFilterFactory.class);
   }

}
