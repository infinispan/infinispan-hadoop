package org.infinispan.hadoop.testutils.mapreduce;

import org.apache.hadoop.mapreduce.Mapper;
import org.infinispan.hadoop.testutils.domain.WebPage;

import java.io.IOException;

/**
 * Mapper using only externalizable objects
 */
public class InfinispanMapper extends Mapper<Integer, WebPage, String, Integer> {
   @Override
   protected void map(Integer key, WebPage webPage, Context context) throws IOException, InterruptedException {
      context.write(webPage.getCategory(), 1);
   }
}