package org.infinispan.hadoop.testutils;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.infinispan.hadoop.testutils.domain.WebPage;
import org.infinispan.hadoop.testutils.hadoop.MiniHadoopCluster;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Utility class containing general methods for tests.
 *
 * @author amanukya
 */
public class Utils {
    /**
     * Saves the objects into HDFS.
     *
     * @param miniHadoopCluster         The HDFS instance.
     * @param webPages                  The list of objects which should be saved.
     * @throws IOException
     */
    public static void saveToHDFS(MiniHadoopCluster miniHadoopCluster , List<WebPage> webPages) throws IOException {
        FileSystem fileSystem = miniHadoopCluster.getFileSystem();
        OutputStream os = fileSystem.create(new Path("/input"));
        BufferedWriter br = new BufferedWriter(new OutputStreamWriter(os, "UTF-8"));
        for (WebPage webPage : webPages) {
            br.write(webPage.getAddress() + "|" + webPage.getCategory() + "\n");
        }
        br.close();
    }

    /**
     * Reads the results of Map Reduce execution from HDFS path.
     * @param miniHadoopCluster         the HDFS instance.
     * @return                          Map of String, Integer pair.
     * @throws IOException
     */
    public static Map<String, Integer> readResultFromHDFS(MiniHadoopCluster miniHadoopCluster, String filePath) throws IOException {
        FileSystem fileSystem = miniHadoopCluster.getFileSystem();

        Map<String, Integer> map = new HashMap<>();
        Path path = new Path(filePath + "/part-r-00000");
        FSDataInputStream fsDataInputStream = fileSystem.open(path);
        BufferedReader br = new BufferedReader(new InputStreamReader(fsDataInputStream));
        String line;
        while ((line = br.readLine()) != null) {
            String[] wordCount = line.split("\t");
            map.put(wordCount[0], Integer.parseInt(wordCount[1]));
        }
        br.close();
        return map;
    }
}