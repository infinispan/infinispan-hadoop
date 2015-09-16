package org.infinispan.hadoop.sample.util;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.impl.ConfigurationProperties;

import java.util.Map;
import java.util.Queue;

/**
 * Utility class to control remote cache contents.
 *
 * @author Pedro Ruivo
 * @since 0.1
 */
public enum Argument {
   HOST("--host", true, "localhost"),
   PORT("--port", true, Integer.toString(ConfigurationProperties.DEFAULT_HOTROD_PORT)),
   CACHE_NAME("--cachename", true, RemoteCacheManager.DEFAULT_CACHE_NAME),
   FILE("--file", true, null),
   POPULATE("--populate", false, null),
   DUMP("--dump", false, null),
   CLEAR("--clear", false, null),
   HELP("--help", false, null);

   private final String arg;
   private final boolean hasValue;
   private final String defaultValue;

   Argument(String arg, boolean hasValue, String defaultValue) {
      this.arg = arg;
      this.hasValue = hasValue;
      this.defaultValue = defaultValue;
   }

   public String getArg() {
      return arg;
   }

   public static void setDefaultValues(Map<Argument, String> map) {
      for (Argument argument : values()) {
         if (argument.defaultValue != null) {
            map.put(argument, argument.defaultValue);
         }
      }
   }

   public static void parse(Queue<String> args, Map<Argument, String> map) {
      String arg = args.poll();
      if (arg == null) {
         return;
      }
      for (Argument argument : values()) {
         if (arg.equals(argument.arg)) {
            if (argument.hasValue) {
               String value = args.poll();
               if (value == null) {
                  throw new IllegalArgumentException("Argument " + arg + " expects a value!");
               }
               map.put(argument, value);
            } else {
               map.put(argument, Boolean.toString(true));
            }
            return;
         }
      }
   }

   public static String help(Argument argument) {
      StringBuilder builder = new StringBuilder();
      builder.append(argument.arg);
      builder.append(" ");
      if (argument.hasValue) {
         builder.append("<value> ");
      }
      if (argument.defaultValue != null) {
         builder.append("(default=");
         builder.append(argument.defaultValue);
         builder.append(")");
      }
      return builder.toString();
   }
}
