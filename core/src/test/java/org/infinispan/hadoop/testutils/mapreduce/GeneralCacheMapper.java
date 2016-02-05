package org.infinispan.hadoop.testutils.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.infinispan.hadoop.testutils.domain.SimpleDomain;
import org.infinispan.hadoop.testutils.domain.WebPage;

import java.io.IOException;

/**
 * Mapper reducer pair using mixed externalizable/writable objects
 */
public class GeneralCacheMapper extends Mapper<Integer, Object, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    @Override
    protected void map(Integer key, Object domain, Context context) throws IOException, InterruptedException {
        if (domain instanceof SimpleDomain) {
            word.set(((SimpleDomain) domain).getValue());
        } else if (domain instanceof WebPage) {
            word.set(((WebPage) domain).getCategory());
        }
        context.write(word, one);
    }
}
