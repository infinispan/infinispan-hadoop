package org.infinispan.hadoop;

import org.apache.hadoop.mapreduce.InputSplit;
import org.infinispan.client.hotrod.CacheTopologyInfo;
import org.infinispan.hadoop.impl.InfinispanInputSplit;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Custom splitter implementation that generates 2 splits per server
 */
public class CustomSplitter implements InfinispanSplitter {

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
         inputSplits.add(new InfinispanInputSplit(s1, ((InetSocketAddress) server.getKey())));
         inputSplits.add(new InfinispanInputSplit(s2, ((InetSocketAddress) server.getKey())));

         taken.addAll(segments);
      }
      return inputSplits;
   }
}