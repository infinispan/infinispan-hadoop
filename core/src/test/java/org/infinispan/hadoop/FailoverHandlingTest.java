package org.infinispan.hadoop;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.infinispan.arquillian.core.HotRodEndpoint;
import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.filter.KeyValueFilterConverterFactory;
import org.infinispan.hadoop.testutils.Utils;
import org.infinispan.hadoop.testutils.converters.CustomFilterFactory;
import org.infinispan.hadoop.testutils.converters.ToWritableOutputConverter;
import org.infinispan.hadoop.testutils.domain.CategoryStats;
import org.infinispan.hadoop.testutils.domain.WebPage;
import org.infinispan.hadoop.testutils.hadoop.MiniHadoopCluster;
import org.infinispan.hadoop.testutils.mapreduce.CacheMapper;
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

/**
 * Testing the failover cases, i.e. the proper work of Hadoop MapReduce in case if one of the clustered servers will
 * go down.
 *
 * @author amanukya
 */
@RunWith(Arquillian.class)
public class FailoverHandlingTest {

    private static final String INPUT_CACHE_NAME = "default";
    private static final String OUTPUT_CACHE_NAME = "repl";
    public static final int NUMBER_OF_ENTRIES = 100000;

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

    private Map<String, Integer> expectedResultMap = new HashMap<String, Integer>();

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
        controller.start("container1");
        controller.start("container2");

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

        createDataAndSaveToCache();
    }

    private List<WebPage> createDataAndSaveToCache() throws MalformedURLException {
        System.out.println("Creating data .. ");
        List<WebPage> webPages = new ArrayList<>(NUMBER_OF_ENTRIES);
        String[] categoryArr = {"software", "government", "streaming"};

        Random randomGenerator = new Random();
        for (int i = 0; i < NUMBER_OF_ENTRIES; i++) {
            String nextCategory = categoryArr[randomGenerator.nextInt(categoryArr.length)];
            WebPage webPage = new WebPage(new URL("http://www.jboss.org"), nextCategory, 1000L + i);
            webPages.add(webPage);

            if (expectedResultMap.get(nextCategory) != null) {
                int currentVal = expectedResultMap.get(nextCategory);
                expectedResultMap.put(nextCategory, ++currentVal);
            } else {
                expectedResultMap.put(nextCategory, 1);
            }

            inputCache.put(i + 1, webPage);
        }

        return webPages;
    }

    @After
    public void stop() throws Exception {
        if (controller.isStarted("container1")) {
            controller.stop("container1");
        }

        if (controller.isStarted("container2")) {
            controller.stop("container2");
        }
    }

    /**
     * Testing the shutdown of the preferred server while mapreduce task execution.
     * @throws Exception
     */
    @Test
    public void testReadFromInfinispanSaveToHDFS_StopPreferedServer() throws Exception {
        System.out.println("Running FailoverHandlingTest#testReadFromInfinispanSaveToHDFS_StopPreferedServer");
        executeTest("container1", false);
    }

    /**
     * Testing the shutdown of one of the clustered servers during the mapreduce task execution.
     * @throws Exception
     */
    @Test
    public void testReadFromInfinispanSaveToHDFS_StopServerFromCluster() throws Exception {
        System.out.println("Running FailoverHandlingTest#testReadFromInfinispanSaveToHDFS_StopServerFromCluster");

        executeTest("container2", false);
    }

    /**
     * Testing the shutdown of one of the clustered servers during the mapreduce task execution. The write of result is
     * performed to Infinispan cache.
     *
     * @throws Exception
     */
    @Test
    public void testReadFromInfinispanSaveToInfinispan_StopOutputServer() throws Exception {
        System.out.println("Running FailoverHandlingTest#testReadFromInfinispanSaveToInfinispan_StopOutputServer");

        executeTest("container2", true);
    }

    private void executeTest(String containerName, boolean isInfinispanWrite) throws Exception{
        Configuration configuration = createBaseConfiguration();

        if (isInfinispanWrite) {
            configuration.set(JobContext.KEY_COMPARATOR, Text.Comparator.class.getName());
        } else {
            configuration.set(InfinispanConfiguration.OUTPUT_KEY_VALUE_CONVERTER, ToWritableOutputConverter.class.getName());
        }

        Job job = Job.getInstance(configuration);

        if (isInfinispanWrite) {
            job.setInputFormatClass(InfinispanInputFormat.class);
            job.setOutputFormatClass(InfinispanOutputFormat.class);

            job.setMapperClass(InfinispanMapper.class);
            job.setMapOutputValueClass(Integer.class);
            job.setReducerClass(InfinispanReducer.class);

            job.setOutputKeyClass(String.class);
            job.setOutputValueClass(CategoryStats.class);

        } else {
            job.setMapperClass(CacheMapper.class);
            job.setReducerClass(HDFSReducer.class);

            job.setInputFormatClass(InfinispanInputFormat.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            FileOutputFormat.setOutputPath(job, new Path("/output-" + containerName + "-" + isInfinispanWrite));
        }

        job.setNumReduceTasks(1);
        job.submit();

        //Waiting for a second and then stopping preferred server.
        Thread.sleep(1200);
        if (!job.isComplete()) {
            System.out.println("Stopping server .." + containerName);
            controller.stop(containerName);
        }

        while (!job.isComplete()) {
            System.out.println("Sleeping for a second...");
            Thread.sleep(1000);
        }

        if (isInfinispanWrite) {
            String host1Address = server1.getHotrodEndpoint().getInetAddress().getHostAddress();
            int port1 = server1.getHotrodEndpoint().getPort();
            RemoteCache<String, CategoryStats> resultCache = new RemoteCacheManager(
                    new ConfigurationBuilder().addServer().host(host1Address).port(port1).build()).getCache(OUTPUT_CACHE_NAME);

            assertEquals(expectedResultMap.get("streaming"), resultCache.get("streaming").getCount());
            assertEquals(expectedResultMap.get("software"), resultCache.get("software").getCount());
            assertEquals(expectedResultMap.get("government"), resultCache.get("government").getCount());
        } else {
            Map<String, Integer> resultMap = Utils.readResultFromHDFS(miniHadoopCluster, "/output-" + containerName + "-" + isInfinispanWrite);
            assertEquals(expectedResultMap.get("streaming").intValue(), resultMap.get("streaming").intValue());
            assertEquals(expectedResultMap.get("software").intValue(), resultMap.get("software").intValue());
            assertEquals(expectedResultMap.get("government").intValue(), resultMap.get("government").intValue());
        }
    }

    private static Archive<?> createFilterArchive() throws IOException {
        return ShrinkWrap.create(JavaArchive.class, "server-filter.jar")
                .addClasses(CustomFilterFactory.class, CustomFilterFactory.GovernmentFilter.class, WebPage.class)
                .addAsServiceProvider(KeyValueFilterConverterFactory.class, CustomFilterFactory.class);
    }
}
