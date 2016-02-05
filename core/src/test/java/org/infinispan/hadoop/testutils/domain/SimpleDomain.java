package org.infinispan.hadoop.testutils.domain;

import org.infinispan.commons.marshall.Externalizer;
import org.infinispan.commons.marshall.SerializeWith;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * // Another simple domain for testing the multiple objects filtering using Hadoop IO.
 *
 * @author amanukya
 */
@SerializeWith(value = SimpleDomain.CustomExternalizer.class)
public class SimpleDomain {
    private String value;

    public SimpleDomain(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SimpleDomain that = (SimpleDomain) o;

        return value != null ? value.equals(that.value) : that.value == null;

    }

    @Override
    public int hashCode() {
        return value != null ? value.hashCode() : 0;
    }

    public static class CustomExternalizer implements Externalizer<SimpleDomain> {
        @Override
        public void writeObject(ObjectOutput output, SimpleDomain page) throws IOException {
            output.writeUTF(page.value);
        }

        @Override
        public SimpleDomain readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            String category = input.readUTF();
            return new SimpleDomain(category);
        }
    }
}