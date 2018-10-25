package org.infinispan.hadoop.testutils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.infinispan.hadoop.InputOutputFormatTest;
import org.infinispan.hadoop.testutils.domain.WebPage;
import org.infinispan.hadoop.testutils.hadoop.MiniHadoopCluster;
import org.junit.Assert;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

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

    private static class FirstChildFileVisitor implements java.nio.file.FileVisitor<java.nio.file.Path> {
        private final String parentName;

        private File childDir;

        FirstChildFileVisitor(String parentName) {
            this.parentName = parentName;
        }

        @Override
        public FileVisitResult preVisitDirectory(java.nio.file.Path dir, BasicFileAttributes attrs) {
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult visitFile(java.nio.file.Path file, BasicFileAttributes attrs) {
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult visitFileFailed(java.nio.file.Path file, IOException exc) {
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult postVisitDirectory(java.nio.file.Path dir, IOException exc) {
            if (dir.toFile().getName().equals(parentName)) {
                File[] files = dir.toFile().listFiles();
                if (files != null && files.length > 0) {
                    childDir = files[0];
                }
            }
            return FileVisitResult.CONTINUE;
        }

        File getChildFolder() {
            return childDir;
        }
    }

    /**
     * Finds the server location given a parent folder
     */
    public static File findServerPath(String parentFolder) throws IOException {
        ClassLoader classLoader = InputOutputFormatTest.class.getClassLoader();
        File file = new File(classLoader.getResource("arquillian.xml").getFile());
        java.nio.file.Path parent = file.toPath().getParent().getParent();
        FirstChildFileVisitor firstChildFileVisitor = new FirstChildFileVisitor(parentFolder);
        Files.walkFileTree(parent, firstChildFileVisitor);
        File childFolder = firstChildFileVisitor.getChildFolder();
        if (childFolder == null) {
            Assert.fail("Could not locate the server under folder " + parentFolder);
        }
        return childFolder;
    }

    /**
     * Adds a <modules><module name=deployment.${moduleName}/></modules> to the cacheManager in the server configFile
     * in case it does not exist.
     */
    public static void addCacheManagerModuleDep(File configFile, String moduleName) throws Exception {
        Document doc = buildDoc(configFile);
        Node cacheManager = doc.getElementsByTagName("cache-container").item(0);

        NodeList childNodes = cacheManager.getChildNodes();
        for (int i = 0; i < childNodes.getLength(); i++) {
            if (childNodes.item(i).getNodeName().equals("modules")) {
                return;
            }
        }

        Element module = doc.createElement("module");
        module.setAttribute("name", "deployment." + moduleName);
        Element modules = doc.createElement("modules");
        modules.appendChild(module);

        cacheManager.appendChild(modules);

        saveDocToFile(doc, configFile);
    }

    private static void saveDocToFile(Document doc, File configFile) throws TransformerException {
        TransformerFactory factory = TransformerFactory.newInstance();
        Transformer transformer = factory.newTransformer();
        Result result = new StreamResult(configFile);
        Source source = new DOMSource(doc);
        transformer.transform(source, result);
    }

    private static Document buildDoc(File configFile) throws Exception {
        DocumentBuilderFactory domFactory = DocumentBuilderFactory.newInstance();
        domFactory.setIgnoringComments(true);
        DocumentBuilder builder = domFactory.newDocumentBuilder();
        return builder.parse(configFile);
    }

    /**
     * Remove the <modules> config from the cacheManager in the server configFile.
     */
    public static void removeCacheManagerModuleDep(File configFile) throws Exception {
        Document doc = buildDoc(configFile);

        Node cacheManager = doc.getElementsByTagName("cache-container").item(0);

        NodeList childNodes = cacheManager.getChildNodes();
        for (int i = 0; i < childNodes.getLength(); i++) {
            Node childItem = childNodes.item(i);
            if (childItem.getNodeName().equals("modules")) {
                cacheManager.removeChild(childItem);
            }
        }
        saveDocToFile(doc, configFile);
    }
}