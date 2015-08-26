package org.infinispan.hadoop.testutils.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

/**
 * Start single JVM Yarn cluster with HDFS, ResourceManager and MapReduce for testing.
 *
 * @author gustavonalle
 */
public class MiniHadoopCluster {

   private FileSystem fileSystem;
   private MiniYARNCluster miniYARNCluster;
   private MiniDFSCluster hdfsCluster;
   private Configuration configuration;

   public void start() throws IOException {
      YarnConfiguration clusterConf = new YarnConfiguration();
      final File hdfsBase = Files.createTempDirectory("temp-hdfs-").toFile();
      clusterConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, hdfsBase.getAbsolutePath());
      hdfsCluster = new MiniDFSCluster.Builder(clusterConf).nameNodeHttpPort(57000).startupOption(HdfsServerConstants.StartupOption.REGULAR).build();
      clusterConf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 64);
      clusterConf.setClass(YarnConfiguration.RM_SCHEDULER, FifoScheduler.class, ResourceScheduler.class);
      miniYARNCluster = new MiniYARNCluster("testMRJOb", 1, 1, 1);
      miniYARNCluster.init(clusterConf);
      miniYARNCluster.start();
      configuration = miniYARNCluster.getConfig();
      fileSystem = new Path("hdfs://localhost:" + hdfsCluster.getNameNodePort() + "/").getFileSystem(configuration);
   }


   public void shutDown() {
      miniYARNCluster.stop();
      hdfsCluster.shutdown();
   }

   public FileSystem getFileSystem() {
      return fileSystem;
   }

   public Configuration getConfiguration() {
      return configuration;
   }
}
