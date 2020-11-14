package com.zwq.infinity.config;

import lombok.extern.slf4j.Slf4j;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

@Slf4j
public class InfinityConfig {
    public static Map<String, Object> parseResourceFile(String filename) throws IOException {
        Yaml yaml = new Yaml();
        InputStream is = null;
        Map<String, Object> configs;
        try {
            is = Thread.currentThread().getContextClassLoader().getResourceAsStream("config/" + filename);
            log.info("启动的配置文件为:{}", filename);
            configs = yaml.load(is);
        } finally {
            if (is != null) {
                is.close();
            }
        }
        if (configs.get("inputs") == null || configs.get("outputs") == null) {
            log.info("Error: No inputs or outputs!");
            throw new RuntimeException("Error: No inputs or outputs!");
        }
        return configs;
    }


    public static Map<String, Object> parseConfigFile(String filename) throws IOException {
        Yaml yaml = new Yaml();
        InputStream is = null;
        Map<String, Object> configs;
        try {
            is = new FileInputStream(new File(filename));
            log.info("启动的配置文件为:{}", filename);
            configs = yaml.load(is);
        } finally {
            if (is != null) {
                is.close();
            }
        }
        if (configs.get("inputs") == null || configs.get("outputs") == null) {
            log.info("Error: No inputs or outputs!");
            throw new RuntimeException("Error: No inputs or outputs!");
        }
        return configs;
    }

    public static Map<String, Object> parseFromProperty(String config) {
        Map<String, Object> configs = new Yaml().load(config);
        if (configs.get("inputs") == null || configs.get("outputs") == null) {
            log.info("Error: No inputs or outputs!");
            throw new RuntimeException("Error: No inputs or outputs!");
        }
        return configs;
    }

}
