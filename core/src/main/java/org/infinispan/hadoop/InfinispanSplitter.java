package org.infinispan.hadoop;


import org.apache.hadoop.mapreduce.InputSplit;
import org.infinispan.client.hotrod.CacheTopologyInfo;

import java.io.IOException;
import java.util.List;

/**
 * Calculate {@link InputSplit} based on the cache topology.
 *
 * @author gustavonalle
 * @since 0.1
 */
public interface InfinispanSplitter {

   List<InputSplit> calculateSplits(CacheTopologyInfo cacheTopologyInfo) throws IOException;
}
