package org.infinispan.hadoop.impl;

import org.apache.hadoop.mapreduce.InputSplit;
import org.infinispan.client.hotrod.CacheTopologyInfo;
import org.infinispan.hadoop.InfinispanSplitter;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * DefaultSplitter will create one split per server, containing only segments present in the server.
 *
 * @author gustavonalle
 * @since 0.1
 */
public final class DefaultSplitter implements InfinispanSplitter {

   @Override
   public List<InputSplit> calculateSplits(CacheTopologyInfo cacheTopologyInfo) throws IOException {
      Map<SocketAddress, Set<Integer>> segmentsPerServer = cacheTopologyInfo.getSegmentsPerServer();
      int numServers = segmentsPerServer.keySet().size();
      if (segmentsPerServer.isEmpty()) {
         throw new IOException("No servers found to partition");
      }
      List<InputSplit> splits = new ArrayList<>();
      if (numServers == 1) {
         SocketAddress serverAddress = segmentsPerServer.keySet().iterator().next();
         InetSocketAddress inetSocketAddress = (InetSocketAddress) serverAddress;
         splits.add(new InfinispanInputSplit(segmentsPerServer.get(serverAddress), inetSocketAddress));
         return splits;
      }

      Set<SocketAddress> servers = segmentsPerServer.keySet();

      int numSegments = cacheTopologyInfo.getNumSegments();

      Set<Integer> takenSegments = new HashSet<>();
      Map<SocketAddress, Set<Integer>> partitions = new HashMap<>();
      while (takenSegments.size() != numSegments) {
         int beforeSize = takenSegments.size();
         for (SocketAddress server : servers) {
            Set<Integer> ownedSegments = segmentsPerServer.get(server);
            if (!ownedSegments.isEmpty()) {
               Iterator<Integer> iterator = ownedSegments.iterator();
               boolean match = false;
               while (iterator.hasNext() && !match) {
                  Integer segment = iterator.next();
                  if (!takenSegments.contains(segment)) {
                     Set<Integer> partitionSegments = partitions.containsKey(server) ? partitions.get(server) : new HashSet<Integer>();
                     partitionSegments.add(segment);
                     partitions.put(server, partitionSegments);
                     takenSegments.add(segment);
                     match = true;
                  }
               }
            }
         }
         if (takenSegments.size() == beforeSize) {
            break;
         }
      }

      for (Entry<SocketAddress, Set<Integer>> entry : partitions.entrySet()) {
         Set<Integer> segments = entry.getValue();
         splits.add(new InfinispanInputSplit(segments, (InetSocketAddress) entry.getKey()));
      }

      return splits;
   }


}
