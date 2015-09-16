package org.infinispan.hadoop.sample.util;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.commons.util.CloseableIterator;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Command line utility for Remote Caches.
 *
 * @author Pedro Ruivo
 * @author gustavonalle
 * @since 0.1
 */
public class ControllerCache {

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
      } else if (map.containsKey(Argument.POPULATE)) {
         System.exit(executeOnCache(new CopyFile(), map));
      } else if (map.containsKey(Argument.DUMP)) {
         System.exit(executeOnCache(new DumpCache(), map));
      } else if (map.containsKey(Argument.CLEAR)) {
         System.exit(executeOnCache(new ClearCache(), map));
      }
   }

   private static <K, V> int executeOnCache(RemoteCacheRunnable<K, V> runnable, Map<Argument, String> map) throws Exception {
      RemoteCacheManager remoteCacheManager = new RemoteCacheManager(map.get(Argument.HOST),
              Integer.parseInt(map.get(Argument.PORT)));
      RemoteCache<K, V> remoteCache = remoteCacheManager.getCache(map.get(Argument.CACHE_NAME));
      try {
         if (remoteCache == null) {
            System.err.println("Unable to connect to cache");
            return -1;
         }
         return runnable.execute(remoteCache, map);
      } finally {
         if (remoteCache != null) {
            remoteCache.stop();
         }
         remoteCacheManager.stop();
      }
   }

   public interface RemoteCacheRunnable<K, V> {
      int execute(RemoteCache<K, V> remoteCache, Map<Argument, String> map) throws Exception;
   }

   private static class CopyFile implements RemoteCacheRunnable<Integer, String> {

      @Override
      public int execute(final RemoteCache<Integer, String> remoteCache, Map<Argument, String> map) throws Exception {
         String filePath = map.get(Argument.FILE);
         if (filePath == null) {
            System.err.println(Argument.FILE.getArg() + " is missing!");
            return 1;
         }

         File file = new File(filePath);
         if (!file.exists()) {
            System.err.println("File '" + filePath + "' not found!");
            return 1;
         }
         BufferedReader reader = new BufferedReader(new FileReader(file));
         ExecutorService executorService = executorService();


         String line;
         int lineNumber = 1;
         while ((line = reader.readLine()) != null) {
            final String finalLine = line;
            final int finalLineNumber = lineNumber++;
            executorService.execute(new Runnable() {
               @Override
               public void run() {
                  remoteCache.put(finalLineNumber, finalLine);
                  if (finalLineNumber % 1000 == 0) {
                     System.out.println("Line " + finalLineNumber + " added by " + Thread.currentThread());
                  }
               }
            });
         }

         reader.close();
         executorService.shutdown();
         while (!executorService.awaitTermination(1, TimeUnit.MINUTES)) {
         }
         return 0;
      }
   }

   private static class DumpCache implements RemoteCacheRunnable<Object, Object> {

      @Override
      public int execute(RemoteCache<Object, Object> remoteCache, Map<Argument, String> map) throws Exception {
         try (CloseableIterator<Map.Entry<Object, Object>> closeableIterator = remoteCache.retrieveEntries(null, 2000)) {
            while (closeableIterator.hasNext()) {
               Map.Entry<Object, Object> entry = closeableIterator.next();
               System.out.println(entry.getKey() + "\t" + entry.getValue());
            }
         }
         return 0;
      }
   }

   private static class ClearCache implements RemoteCacheRunnable<Object, Object> {

      @Override
      public int execute(RemoteCache<Object, Object> remoteCache, Map<Argument, String> map) throws Exception {
         remoteCache.clear();
         return 0;
      }
   }

   private static ExecutorService executorService() {
      return new ThreadPoolExecutor(1, Runtime.getRuntime().availableProcessors() * 2, 10, TimeUnit.SECONDS,
              new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
   }
}
