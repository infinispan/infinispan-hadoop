package org.infinispan.hadoop.impl;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static org.apache.hadoop.io.WritableUtils.readVInt;
import static org.apache.hadoop.io.WritableUtils.writeVInt;

/**
 * An input split that represents a portion of the data in a cache, defined by a server location and a set
 * of segments.
 *
 * @author gustavonalle
 * @since 0.1
 */
public class InfinispanInputSplit extends InputSplit implements Writable {

   private String preferredLocation;
   private Set<Integer> segments = new HashSet<>();

   public InfinispanInputSplit(Set<Integer> segments, String preferredLocation) {
      this.segments.addAll(segments);
      this.preferredLocation = preferredLocation;
   }

   @SuppressWarnings("unused")
   public InfinispanInputSplit() {
   }

   @Override
   public long getLength() throws IOException, InterruptedException {
      return 0;
   }

   @Override
   public String[] getLocations() throws IOException, InterruptedException {
      return new String[]{preferredLocation};
   }

   public Set<Integer> getSegments() {
      return segments;
   }

   @Override
   public void write(DataOutput out) throws IOException {
      Text.writeString(out, preferredLocation);
      writeVInt(out, segments.size());
      for (Integer segment : segments) {
         writeVInt(out, segment);
      }
   }

   @Override
   public void readFields(DataInput in) throws IOException {
      preferredLocation = Text.readString(in);
      int segmentSize = readVInt(in);
      for (int i = 0; i < segmentSize; i++) {
         segments.add(readVInt(in));
      }
   }
}
