package com.sample.assignment.batch.common;

import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class ApplicationPropertyLoader {
    static final Logger logger = Logger.getLogger(ApplicationPropertyLoader.class);
    private static ApplicationPropertyLoader instance = null;

    private ApplicationPropertyLoader() {
    }

    public static synchronized ApplicationPropertyLoader getInstance() {
        if (instance == null) {
            instance = new ApplicationPropertyLoader();
        }
        return instance;
    }

    public synchronized Map<String, String> getpropertiesMap(String configFile) throws Exception {
        logger.info("Loading application properties from config file " + configFile);
        Map<String, String> propMap = new HashMap<String, String>();
        InputStream inputStream = new FileInputStream(configFile);
        Properties prop = new Properties();
        prop.load(inputStream);
        logger.info("Application config loaded successfully");

        Enumeration<?> e = prop.propertyNames();
        String key = null;
        while (e.hasMoreElements()) {
            key = (String) e.nextElement();
            propMap.put(key, prop.getProperty(key));
        }
        inputStream.close();
        return propMap;
    }
}
