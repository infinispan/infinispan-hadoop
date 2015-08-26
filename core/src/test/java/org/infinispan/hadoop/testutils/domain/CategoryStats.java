package org.infinispan.hadoop.testutils.domain;

import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.Externalizer;
import org.infinispan.commons.marshall.SerializeWith;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author gustavonalle
 */
@SerializeWith(value = CategoryStats.CategoryStatsExternalizer.class)
public class CategoryStats {
   private Integer count;

   public CategoryStats(Integer count) {
      this.count = count;
   }


   public Integer getCount() {
      return count;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      CategoryStats that = (CategoryStats) o;

      return count.equals(that.count);

   }

   @Override
   public int hashCode() {
      return count.hashCode();
   }

   public static class CategoryStatsExternalizer implements Externalizer<CategoryStats> {


      @Override
      public void writeObject(ObjectOutput output, CategoryStats categoryStats) throws IOException {
         UnsignedNumeric.writeUnsignedInt(output, categoryStats.count);
      }

      @Override
      public CategoryStats readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         int count = UnsignedNumeric.readUnsignedInt(input);
         return new CategoryStats(count);
      }

   }
}
