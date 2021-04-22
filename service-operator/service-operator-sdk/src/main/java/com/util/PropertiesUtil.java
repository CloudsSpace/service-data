package com.util;

import scala.collection.JavaConversions;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

/**
 * 加载Properties配置文件
 *
 * @Author: ysh
 * @Date: 2019/5/31 14:25
 * @Version: 1.0
 */
public  class PropertiesUtil {

    public static Map<String, String> getJavaMap(String fileName) {
        Properties properties = getProperties(fileName);
        Map<String, String> javaMap = new HashMap<String, String>((Map) properties);
        return javaMap;
    }

    public static scala.collection.mutable.Map<String, String> getScalaMap(String fileName) {
        Map<String, String> javaMap = getJavaMap(fileName);
        scala.collection.mutable.Map<String, String> scalaMap = JavaConversions.mapAsScalaMap(javaMap);
        return scalaMap;
    }

    public static Properties mapToProperties(Map<String, String> map) {
        if (map == null) {
            return null;
        }
        Properties properties = new Properties();
        Set<Map.Entry<String, String>> entries = map.entrySet();
        for(Map.Entry<String, String> entry : entries){
            properties.setProperty(entry.getKey(),entry.getValue());
        }
        return properties;
    }


    public static Properties getProperties(String fileName) {
        InputStream stream = null;
        Properties properties = new Properties();
        try {
            stream = Thread.currentThread().getContextClassLoader().getResourceAsStream(fileName);
            properties.load(stream);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (stream != null) {
                    stream.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return properties;
    }

    public static List<String> getFile(String fileName) {
        InputStream inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(fileName);
        Scanner scanner = new Scanner(inputStream);
        List<String> list = new ArrayList<String>();
        while (scanner.hasNext()) {
            String line = scanner.nextLine();
            list.add(line);
        }
        return list;
    }




}
