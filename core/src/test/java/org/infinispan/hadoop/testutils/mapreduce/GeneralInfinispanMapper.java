package org.infinispan.hadoop.testutils.mapreduce;

import org.apache.hadoop.mapreduce.Mapper;
import org.infinispan.hadoop.testutils.domain.SimpleDomain;
import org.infinispan.hadoop.testutils.domain.WebPage;

import java.io.IOException;

/**
 * Mapper reducer pair using only externalizable objects
 */
public class GeneralInfinispanMapper extends Mapper<Integer, Object, String, Integer> {
    @Override
    protected void map(Integer key, Object domain, Context context) throws IOException, InterruptedException {
        if (domain instanceof SimpleDomain) {
            context.write(((SimpleDomain) domain).getValue(), 1);
        } else if (domain instanceof WebPage) {
            context.write(((WebPage) domain).getCategory(), 1);
        }
    }
}
