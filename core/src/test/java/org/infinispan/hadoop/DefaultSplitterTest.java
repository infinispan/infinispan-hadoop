package org.infinispan.hadoop;

import org.apache.hadoop.mapreduce.InputSplit;
import org.infinispan.client.hotrod.CacheTopologyInfo;
import org.infinispan.hadoop.impl.DefaultSplitter;
import org.infinispan.hadoop.impl.InfinispanInputSplit;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author gustavonalle
 */
public class DefaultSplitterTest {

   DefaultSplitter defaultSplitter = new DefaultSplitter();

   private static final int TEST_PORT = 80;

   @Test(expected = IOException.class)
   public void testEmpty() throws Exception {
      CacheTopologyInfo cacheTopologyInfo = new CacheTopologyInfoBuilder()
              .withNumSegments(20)
              .build();

      defaultSplitter.calculateSplits(cacheTopologyInfo);
   }

   @Test
   public void testSingleServer() throws Exception {
      CacheTopologyInfo cacheTopologyInfo = new CacheTopologyInfoBuilder()
              .addServer("server1", intRange(20))
              .withNumSegments(20)
              .build();

      List<InputSplit> splits = defaultSplitter.calculateSplits(cacheTopologyInfo);

      assertSplits(splits, cacheTopologyInfo);
   }

   @Test
   public void testReplicatedTopology() throws Exception {
      int numServers = 3;
      int numSegments = 60;
      CacheTopologyInfo cacheTopologyInfo = new CacheTopologyInfoBuilder()
              .replicated(numSegments, numServers)
              .build();

      List<InputSplit> splits = defaultSplitter.calculateSplits(cacheTopologyInfo);

      assertEquals(3, splits.size());
      assertSplits(splits, cacheTopologyInfo);
   }

   @Test
   public void testDistributedTopology() throws Exception {
      int numServers = 4;
      int numOwners = 2;
      int numSegments = 60;
      CacheTopologyInfo cacheTopologyInfo = new CacheTopologyInfoBuilder()
              .distributed(numSegments, numServers, numOwners)
              .build();

      List<InputSplit> splits = defaultSplitter.calculateSplits(cacheTopologyInfo);

      assertEquals(numServers, splits.size());
      assertSplits(splits, cacheTopologyInfo);
   }

   @Test
   public void testDistributedTopology2() throws Exception {
      int numServers = 5;
      int numOwners = 2;
      int numSegments = 11;
      CacheTopologyInfo cacheTopologyInfo = new CacheTopologyInfoBuilder()
              .distributed(numSegments, numServers, numOwners)
              .build();

      List<InputSplit> splits = defaultSplitter.calculateSplits(cacheTopologyInfo);

      assertEquals(numServers, splits.size());
      assertSplits(splits, cacheTopologyInfo);
   }

   @Test
   public void testUnevenDistribution() throws Exception {
      CacheTopologyInfo cacheTopologyInfo = new CacheTopologyInfoBuilder()
              .addServer("server1", 0, 1, 2)
              .addServer("server2", 1)
              .addServer("server3", 2)
              .withNumSegments(3)
              .build();

      List<InputSplit> splits = defaultSplitter.calculateSplits(cacheTopologyInfo);

      assertEquals(3, splits.size());
      assertSplits(splits, cacheTopologyInfo);
   }

   @Test
   public void testMissingSegments() throws Exception {
      CacheTopologyInfo cacheTopologyInfo = new CacheTopologyInfoBuilder()
              .addServer("server1", 0, 2)
              .addServer("server2", 3)
              .withNumSegments(4)
              .build();

      List<InputSplit> splits = defaultSplitter.calculateSplits(cacheTopologyInfo);

      assertEquals(2, splits.size());
      assertSplits(splits, cacheTopologyInfo);
   }


   private static Integer[] intRange(int min, int max) {
      Integer[] values = new Integer[max];
      for (int i = min; i < max; i++) {
         values[i] = i;
      }
      return values;
   }

   private Integer[] intRange(int max) {
      return intRange(0, max);
   }

   private void assertSplits(List<InputSplit> splits, CacheTopologyInfo cacheTopologyInfo) throws IOException, InterruptedException {
      Map<SocketAddress, Set<Integer>> segmentsPerServer = cacheTopologyInfo.getSegmentsPerServer();
      int numServers = segmentsPerServer.keySet().size();

      // Assert 1 split per server is created
      assertEquals(numServers, splits.size());

      List<Integer> allSplitSegments = new ArrayList<>();
      for (InputSplit split : splits) {
         InfinispanInputSplit ispnSplit = (InfinispanInputSplit) split;
         allSplitSegments.addAll(ispnSplit.getSegments());
         String host = ispnSplit.getLocations()[0];
         InetSocketAddress server = InetSocketAddress.createUnresolved(host, TEST_PORT);
         // Asert split contains only segments owned by the related server
         for (Integer splitSegment : ispnSplit.getSegments()) {
            assertTrue(segmentsPerServer.get(server).contains(splitSegment));
         }
      }
      // Assert unique segments across splits
      List<Integer> allSegments = uniqueValuesFlattened(segmentsPerServer);
      Collections.sort(allSplitSegments);
      assertEquals(allSegments, allSplitSegments);
   }

   private List<Integer> uniqueValuesFlattened(Map<?, Set<Integer>> source) {
      Set<Integer> values = new HashSet<>();
      for (Map.Entry<?, Set<Integer>> entry : source.entrySet()) {
         values.addAll(entry.getValue());
      }
      ArrayList<Integer> sortedValues = new ArrayList<>(values);
      Collections.sort(sortedValues);
      return sortedValues;
   }

   private static class CacheTopologyInfoBuilder {

      Map<SocketAddress, Set<Integer>> map = new HashMap<>();
      int numSegments;

      CacheTopologyInfoBuilder addServer(String name, Integer... segments) {
         Set<Integer> integers = new HashSet<>();
         Collections.addAll(integers, segments);
         InetSocketAddress server = InetSocketAddress.createUnresolved(name, TEST_PORT);
         map.put(server, integers);
         return this;
      }

      CacheTopologyInfoBuilder withNumSegments(int numSegments) {
         this.numSegments = numSegments;
         return this;
      }

      CacheTopologyInfoBuilder replicated(int numSegments, int numServers) {
         map.clear();
         for (int i = 0; i < numServers; i++) {
            addServer("server" + i, intRange(0, numSegments));
            this.numSegments = numSegments;

         }
         return this;
      }

      CacheTopologyInfoBuilder distributed(int numSegments, int numServers, int numOwners) {
         map.clear();
         this.numSegments = numSegments;

         for (int i = 0; i < numServers; i++) {
            map.put(InetSocketAddress.createUnresolved("server" + i, TEST_PORT), new HashSet<Integer>());
         }

         for (int j = 0; j < numSegments; j++) {
            int bucket = j % numServers;
            for (int i = 0; i < numOwners; i++) {
               int nexBucket = bucket >= numServers - 1 ? 0 : bucket + 1;
               InetSocketAddress key = InetSocketAddress.createUnresolved("server" + bucket, TEST_PORT);
               InetSocketAddress nextKey = InetSocketAddress.createUnresolved("server" + nexBucket, TEST_PORT);
               map.get(key).add(j);
               map.get(nextKey).add(j);
            }
         }
         return this;
      }

      CacheTopologyInfo build() {
         return new CacheTopologyInfo() {
            @Override
            public int getNumSegments() {
               return numSegments;
            }

            @Override
            public Map<SocketAddress, Set<Integer>> getSegmentsPerServer() {
               return map;
            }

            @Override
            public Integer getTopologyId() {
               return 1;
            }
         };
      }
   }

}
