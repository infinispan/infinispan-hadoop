package org.infinispan.hadoop.impl;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Collection;
import java.util.Set;

import org.infinispan.client.hotrod.FailoverRequestBalancingStrategy;
import org.infinispan.client.hotrod.impl.transport.tcp.RoundRobinBalancingStrategy;

/**
 * Request balancing strategy that will prefer the provided server and when not possible will fall back to round robin.
 *
 * @author gustavonalle
 * @since 0.1
 */
public class PreferredServerBalancingStrategy implements FailoverRequestBalancingStrategy {
   private final InetSocketAddress preferredServer;
   private final RoundRobinBalancingStrategy roundRobin = new RoundRobinBalancingStrategy();

   public PreferredServerBalancingStrategy(InetSocketAddress preferredServer) {
      this.preferredServer = preferredServer;
   }

   @Override
   public void setServers(Collection<SocketAddress> servers) {
      roundRobin.setServers(servers);
   }


   private boolean isValid(InetSocketAddress server) {
      for (SocketAddress socketAddress : roundRobin.getServers()) {
         if (socketAddress.equals(server)) {
            return true;
         }
      }
      return false;
   }

   @Override
   public SocketAddress nextServer(Set<SocketAddress> failedServers) {
      if ((failedServers != null && failedServers.contains(preferredServer)) || !isValid(preferredServer)) {
         return roundRobin.nextServer(failedServers);
      }
      return preferredServer;
   }
}