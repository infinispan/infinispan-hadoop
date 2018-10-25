package org.infinispan.hadoop.sample.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;

/**
 * Phrase ingester for remote caches.
 *
 * @author Pedro Ruivo
 * @since 0.1
 */
public class WordCounterPopulator {

   public static void main(String[] args) throws Exception {
      Map<Argument, String> map = new EnumMap<>(Argument.class);
      Queue<String> queue = new LinkedList<>(Arrays.asList(args));
      Argument.setDefaultValues(map);
      while (!queue.isEmpty()) {
         Argument.parse(queue, map);
      }

      if (map.containsKey(Argument.HELP)) {
         System.out.println("The following arguments are allowed:");
         for (Argument argument : Argument.values()) {
            System.out.println(Argument.help(argument));
         }
         System.exit(0);
      }

      String filePath = map.get(Argument.FILE);
      if (filePath == null) {
         System.err.println(Argument.FILE.getArg() + " is missing!");
         System.exit(1);
      }

      File file = new File(filePath);
      if (!file.exists()) {
         System.err.println("File '" + filePath + "' not found!");
         System.exit(1);
      }
      BufferedReader reader = new BufferedReader(new FileReader(file));
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.addServer().host(map.get(Argument.HOST)).port(Integer.parseInt(map.get(Argument.PORT)));
      RemoteCacheManager remoteCacheManager = new RemoteCacheManager(builder.build());
      RemoteCache<Integer, String> remoteCache = remoteCacheManager.getCache(map.get(Argument.CACHE_NAME));

      String line;
      int lineNumber = 1;
      System.out.println();
      while ((line = reader.readLine()) != null) {
         remoteCache.put(lineNumber++, line);
         if (lineNumber % 100 == 0) {
            System.out.print("\rLine " + lineNumber + " added!");
         }
      }
      System.out.println();

      reader.close();
      remoteCache.stop();
      remoteCacheManager.stop();
      System.exit(0);
   }

}
