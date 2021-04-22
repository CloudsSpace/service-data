package com;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.util.Properties;

public class Configs {
    static Properties properties = null;
    static Config config = ConfigFactory.load("application.properties");
    public static String key(String key) {
        return config.getString(key);
    }

    public static Properties mysqlProperties(String groupId) {
        properties= new Properties();
        properties.setProperty("jdbc.driver", config.getString("kafka.bootstrap.servers"));
        properties.setProperty("jdbc.url", config.getString("kafka.zookeeper.connect"));
        properties.setProperty("jdbc.username", config.getString("kafka.zookeeper.connect"));
        properties.setProperty("jdbc.password", config.getString("kafka.zookeeper.connect"));

        properties.setProperty("group.id", groupId);
        return properties;
    }


}
