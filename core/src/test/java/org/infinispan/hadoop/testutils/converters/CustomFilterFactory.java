package org.infinispan.hadoop.testutils.converters;

import org.infinispan.filter.AbstractKeyValueFilterConverter;
import org.infinispan.filter.KeyValueFilterConverter;
import org.infinispan.filter.KeyValueFilterConverterFactory;
import org.infinispan.filter.NamedFactory;
import org.infinispan.hadoop.testutils.domain.WebPage;
import org.infinispan.metadata.Metadata;

import java.io.Serializable;

import static org.infinispan.hadoop.InputOutputFormatTest.GOVERNMENT_PAGE_FILTER_FACTORY;

@NamedFactory(name = GOVERNMENT_PAGE_FILTER_FACTORY)
public class CustomFilterFactory implements KeyValueFilterConverterFactory<Integer, WebPage, WebPage> {
   @Override
   public KeyValueFilterConverter<Integer, WebPage, WebPage> getFilterConverter() {
      return new GovernmentFilter();
   }

   public static class GovernmentFilter extends AbstractKeyValueFilterConverter<Integer, WebPage, WebPage> implements Serializable {
      @Override
      public WebPage filterAndConvert(Integer key, WebPage value, Metadata metadata) {
         String host = value.getAddress().getHost();
         return host.endsWith("gov") || host.endsWith("gov.uk") ? value : null;
      }
   }
}