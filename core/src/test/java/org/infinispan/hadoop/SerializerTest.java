package org.infinispan.hadoop;

import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serializer;
import org.infinispan.hadoop.serialization.JBossMarshallerSerialization;
import org.infinispan.hadoop.testutils.domain.WebPage;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.net.URL;

import static org.junit.Assert.assertEquals;

/**
 * @author gustavonalle
 */
public class SerializerTest {

   @Test
   public void testSerializer() throws Exception {
      WebPage originalWebPage = new WebPage(new URL("http://www.jboss.org"), "opensource", 10L);

      ByteArrayOutputStream baos = new ByteArrayOutputStream();

      JBossMarshallerSerialization<WebPage> marshallerSerialization = new JBossMarshallerSerialization<>();
      Serializer<WebPage> serializer = marshallerSerialization.getSerializer(WebPage.class);
      serializer.open(baos);
      serializer.serialize(originalWebPage);
      serializer.close();

      Deserializer<WebPage> deserializer = marshallerSerialization.getDeserializer(WebPage.class);
      deserializer.open(new ByteArrayInputStream(baos.toByteArray()));
      WebPage deserializedWebPage = deserializer.deserialize(null);
      deserializer.close();

      assertEquals(deserializedWebPage, originalWebPage);
   }
}
