package org.infinispan.hadoop;

/**
 * Converter for Key-value pairs
 *
 * @param <KC> Original key type
 * @param <VC> Original value type
 * @param <K>  Converted key type
 * @param <V>  Converted value type
 * @author gustavonalle
 * @since 0.1
 */
public interface KeyValueConverter<KC, VC, K, V> {

   K convertKey(KC key);

   V convertValue(VC value);

}
