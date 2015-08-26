package org.infinispan.hadoop.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.infinispan.client.hotrod.CacheTopologyInfo;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.hadoop.InfinispanConfiguration;

import java.net.InetSocketAddress;

/**
 * Wrapper around RemoteCache.
 *
 * @author Pedro Ruivo
 * @author gustavonalle
 * @since 0.1
 */
public class InfinispanCache<K, V> {

   private static Log log = LogFactory.getLog(InfinispanCache.class);

   private final RemoteCacheManager remoteCacheManager;
   private final RemoteCache<K, V> remoteCache;

   private InfinispanCache(RemoteCacheManager remoteCacheManager, RemoteCache<K, V> remoteCache) {
      this.remoteCacheManager = remoteCacheManager;
      this.remoteCache = remoteCache;
   }

   public void stop() {
      if (remoteCache != null) {
         remoteCache.stop();
      }
      if (remoteCacheManager != null) {
         remoteCacheManager.stop();
      }
   }

   public RemoteCache<K, V> getRemoteCache() {
      return remoteCache;
   }


   public static <K, V> InfinispanCache<K, V> getInputCache(InfinispanConfiguration configuration) {
      return getCache(configuration.getInputRemoteCacheHost(), configuration.getInputRemoteCachePort(),
              configuration.getInputCacheName());
   }

   public static <K, V> InfinispanCache<K, V> getOutputCache(InfinispanConfiguration configuration) {
      return getCache(configuration.getOutputRemoteCacheHost(), configuration.getOutputRemoteCachePort(),
              configuration.getOutputCacheName());
   }

   private static <K, V> InfinispanCache<K, V> getCache(String host, int port, String name) {
      log.info("Connecting to cache " + name + " in [" + host + ":" + port + "]");
      ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
      configurationBuilder.addServer().host(host).port(port).balancingStrategy(new PreferredServerBalancingStrategy(InetSocketAddress.createUnresolved(host, port)));
      RemoteCacheManager remoteCacheManager = new RemoteCacheManager(configurationBuilder.build());
      RemoteCache<K, V> remoteCache = remoteCacheManager.getCache(name);
      return new InfinispanCache<>(remoteCacheManager, remoteCache);
   }

   @Override
   public String toString() {
      return "InfinispanCache{" +
              "remoteCacheManager=" + remoteCacheManager +
              ", remoteCache=" + remoteCache +
              '}';
   }

   public CacheTopologyInfo getCacheTopology() {
      return remoteCache.getCacheTopologyInfo();
   }
}
