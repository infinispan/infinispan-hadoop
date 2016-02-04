package org.infinispan.hadoop.impl;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetSocketAddress;
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

   private InetSocketAddress preferredServer;
   private Set<Integer> segments = new HashSet<>();

   public InfinispanInputSplit(Set<Integer> segments, InetSocketAddress preferredServer) {
      this.segments.addAll(segments);
      this.preferredServer = preferredServer;
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
      return new String[]{preferredServer.getHostName()};
   }

   public Set<Integer> getSegments() {
      return segments;
   }

   @Override
   public void write(DataOutput out) throws IOException {
      Text.writeString(out, preferredServer.getHostString());
      writeVInt(out, preferredServer.getPort());
      writeVInt(out, segments.size());
      for (Integer segment : segments) {
         writeVInt(out, segment);
      }
   }

   @Override
   public void readFields(DataInput in) throws IOException {
      String hostString = Text.readString(in);
      int port = readVInt(in);
      this.preferredServer = new InetSocketAddress(hostString, port);
      int segmentSize = readVInt(in);
      for (int i = 0; i < segmentSize; i++) {
         this.segments.add(readVInt(in));
      }
   }

   public InetSocketAddress getPreferredServer() {
      return preferredServer;
   }
}
