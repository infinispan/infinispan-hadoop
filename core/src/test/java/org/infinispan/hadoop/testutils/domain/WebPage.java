package org.infinispan.hadoop.testutils.domain;

import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.Externalizer;
import org.infinispan.commons.marshall.SerializeWith;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.net.URL;

/**
 * @author gustavonalle
 */
@SerializeWith(value = WebPage.WebPageExternalizer.class)
public class WebPage {

   private URL address;
   private String category;
   private Long accessCount;

   public WebPage(URL address, String category, Long accessCount) {
      this.address = address;
      this.category = category;
      this.accessCount = accessCount;
   }


   public Long getAccessCount() {
      return accessCount;
   }

   public String getCategory() {
      return category;
   }

   public URL getAddress() {
      return address;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      WebPage webPage = (WebPage) o;

      return address.equals(webPage.address) && category.equals(webPage.category) && accessCount.equals(webPage.accessCount);

   }

   @Override
   public int hashCode() {
      int result = address.hashCode();
      result = 31 * result + category.hashCode();
      result = 31 * result + accessCount.hashCode();
      return result;
   }

   public static class WebPageExternalizer implements Externalizer<WebPage> {
      @Override
      public void writeObject(ObjectOutput output, WebPage page) throws IOException {
         output.writeObject(page.address);
         output.writeUTF(page.category);
         UnsignedNumeric.writeUnsignedLong(output, page.accessCount);
         output.writeObject(page.accessCount);
      }

      @Override
      public WebPage readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         URL url = (URL) input.readObject();
         String category = input.readUTF();
         long accessCount = UnsignedNumeric.readUnsignedLong(input);
         return new WebPage(url, category, accessCount);
      }
   }

}
