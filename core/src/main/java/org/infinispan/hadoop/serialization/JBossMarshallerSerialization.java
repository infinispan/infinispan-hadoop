package org.infinispan.hadoop.serialization;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.Serializer;
import org.infinispan.commons.marshall.jboss.GenericJBossMarshaller;
import org.infinispan.hadoop.InfinispanConfiguration;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStream;

/**
 * Experimental serialization using JBoss Marshaller. To configure which classes are serializable using this mechanism,
 * the property "hadoop.ispn.io.serialization.classes" should be used.
 *
 * @author gustavonalle
 * @since 0.1
 */
public class JBossMarshallerSerialization<T> implements Serialization<T>, Configurable {

   private Configuration conf;
   private GenericJBossMarshaller marshaller = new GenericJBossMarshaller();

   @Override
   public boolean accept(Class<?> c) {
      Configuration conf = getConf();
      String[] classes = conf.getTrimmedStrings(InfinispanConfiguration.SERIALIZATION_CLASSES);
      if (classes == null || classes.length == 0) {
         return false;
      }
      for (String clazz : classes) {
         if (clazz.equals(c.getName())) {
            return true;
         }
      }
      return false;
   }

   @Override
   public Serializer<T> getSerializer(Class<T> c) {
      return new MarshallerSerializer<>(marshaller);
   }

   @Override
   public Deserializer<T> getDeserializer(Class<T> c) {
      return new MarshallerDeserializer<>(marshaller);
   }

   @Override
   public void setConf(Configuration conf) {
      this.conf = conf;
   }

   @Override
   public Configuration getConf() {
      return conf;
   }

   private static class MarshallerSerializer<T> implements Serializer<T> {
      private final GenericJBossMarshaller marshaller;
      private OutputStream out;

      public MarshallerSerializer(GenericJBossMarshaller marshaller) {
         this.marshaller = marshaller;
      }

      @Override
      public void open(OutputStream out) throws IOException {
         this.out = out;
      }

      @Override
      public void serialize(T o) throws IOException {
         ObjectOutput objectOutput = marshaller.startObjectOutput(out, false, 512);
         try {
            marshaller.objectToObjectStream(o, objectOutput);
         } finally {
            marshaller.finishObjectOutput(objectOutput);
         }
      }

      @Override
      public void close() throws IOException {
         marshaller.stop();
      }
   }

   private static class MarshallerDeserializer<T> implements Deserializer<T> {
      private final GenericJBossMarshaller marshaller;
      private InputStream inputStream;

      public MarshallerDeserializer(GenericJBossMarshaller marshaller) {

         this.marshaller = marshaller;
      }

      @Override
      public void open(InputStream in) throws IOException {
         this.inputStream = in;
      }

      @Override
      public void close() throws IOException {
         marshaller.stop();
      }

      @Override
      @SuppressWarnings("unchecked")
      public T deserialize(Object o) throws IOException {
         ObjectInput objectInput = marshaller.startObjectInput(inputStream, false);
         try {
            return (T) marshaller.objectFromObjectStream(objectInput);
         } catch (ClassNotFoundException e) {
            throw new IOException(e);
         } finally {
            marshaller.finishObjectInput(objectInput);
         }
      }
   }
}
