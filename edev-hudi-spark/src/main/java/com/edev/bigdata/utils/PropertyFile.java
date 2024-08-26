package com.edev.bigdata.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Read property file to get its properties.
 * @author fangang
 */
public class PropertyFile {
    private static final String PROP_FILE = "spark.properties";
    private static Properties properties = null;

    /**
     * read the property file
     */
    public void readPropertyFile(){
        try {
            Properties props = new Properties();
            InputStream is = this.getClass().getClassLoader().getResourceAsStream(PROP_FILE);
            if(is==null) throw new RuntimeException("Not found property file: "+PROP_FILE);
            props.load(is);
            properties = props;
        } catch (IOException e) {
            throw new RuntimeException("read property file error!",e);
        }
    }

    /**
     * @param key the key
     * @return the value of the key in the property file.
     */
    public static String getProperty(String key) {
        if(properties==null){
            (new PropertyFile()).readPropertyFile();
        }
        return properties.getProperty(key);
    }

    /**
     * @param key the key
     * @return the value of the key in the property file.
     */
    public static String getProperty(String key,String defaultValue) {
        if(properties==null){
            (new PropertyFile()).readPropertyFile();
        }
        String property = properties.getProperty(key);
        return property==null ? defaultValue : property;
    }
}
