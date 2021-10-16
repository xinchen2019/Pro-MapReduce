package com.apple.utils;

import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.Charset;
import java.util.Map;

/**
 * @Program: Pro-MapReduce
 * @ClassName: PropertiesUtil
 * @Description: TODO
 * @Author Mr.Apple
 * @Create: 2021-03-27 13:00
 * @Version 1.1.0
 **/
public class PropertiesUtil {

    private static Logger logger = LoggerFactory.getLogger(PropertiesUtil.class);

    private static Map<String, String> map = new HashedMap(16);

    /**
     * @Description:1.0.0
     * @Author: Apple
     * @Date: 2021/3/27 18:36
     */
    public static Map<String, String> getPropertiesMap() {
        return map;
    }

    public static void loadProperties(String path) throws IOException {
        BufferedReader breader = null;
        InputStream inputStream = null;
        String line;
        try {
            inputStream = Thread.currentThread()
                    .getContextClassLoader()
                    .getResourceAsStream(path);
            if (inputStream == null) {
                inputStream = new BufferedInputStream(new FileInputStream(path));
            }
            breader = new BufferedReader(new InputStreamReader(inputStream,
                    Charset.forName("UTF-8")));
            line = breader.readLine();
            while (line != null) {
                if (StringUtils.isNotBlank(line) && line.contains("=")) {
                    String key = line.split("=")[0];
                    String value = line.split("=")[1];
                    map.put(key, value);
                }
                line = breader.readLine();
            }
            logger.info("配置文件加载成功:[{}]", path);
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("配置文件加载错误：[{}]", path);
        } finally {
            if (breader != null) {
                try {
                    breader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void main(String[] args) throws IOException {
        String propPath = "prop.properties";
        PropertiesUtil.loadProperties(propPath);
        Map<String, String> propMap = PropertiesUtil.getPropertiesMap();
        System.out.println("value: " + propMap.get("cc"));
    }
}