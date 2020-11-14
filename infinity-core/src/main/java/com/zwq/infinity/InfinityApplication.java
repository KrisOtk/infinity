package com.zwq.infinity;

import com.zwq.infinity.baseplugin.InfinityBuilder;
import com.zwq.infinity.config.InfinityConfig;
import com.zwq.infinity.util.InfinityJsonUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.util.*;

/**
 * @author kris
 */
@Slf4j
public class InfinityApplication {
    public static void main(String[] args) {
        try {
            log.info("infinity消费应用准备启动");
            Map<String, Object> configs;
            if (System.getProperty("useConfigMap") != null && System.getProperty("useConfigMap").equals("true")) {
                //方式1 通过环境变量进行启动,比如启动在k8s容器中启动
                String configMapName = System.getProperty("configMapName");
                String config = System.getenv(configMapName);
                log.info("准备使用环境变量^^^环境变量名称:{}^^^配置:{}", configMapName, config);
                configs = InfinityConfig.parseFromProperty(config);
            } else {
                //根据文件启动,取决与绝对地址或者相对地址或jar包内部配置文件,config.file
                String configFile = System.getProperty("config.file");
                if (StringUtils.isBlank(configFile)) {
                    log.info("未指定要启动的配置文件(config.file=null),即将退出!");
                    System.exit(1);
                }
                if (!(configFile.contains("/") || configFile.contains(".yml"))) {
                    log.info("准备使用jar包内部resources/config目录下的配置文件:{}", configFile + ".yml");
                    configs = InfinityConfig.parseResourceFile(configFile + ".yml");
                } else {
                    log.info("准备使用外部目录下的配置文件进行启动:{}", configFile);
                    configs = InfinityConfig.parseConfigFile(configFile);
                }
            }
            final ArrayList<Map<String, Map<String, Object>>> inputConfigs = (ArrayList<Map<String, Map<String, Object>>>) configs.get("inputs");
            final ArrayList<Map<String, Map<String, Object>>> filterConfigs = (ArrayList<Map<String, Map<String, Object>>>) configs.get("filters");
            final ArrayList<Map<String, Map<String, Object>>> outputConfigs = (ArrayList<Map<String, Map<String, Object>>>) configs.get("outputs");
            final ArrayList<Map<String, Map<String, Object>>> metrics = (ArrayList<Map<String, Map<String, Object>>>) configs.get("metrics");
            InfinityBuilder tb = new InfinityBuilder(inputConfigs, filterConfigs, outputConfigs, metrics);
            log.info("配置详情:{}", InfinityJsonUtil.toJSON(configs));
            tb.build().forEach(input -> new InputEmitThread(input).start());
        } catch (Throwable e) {
            log.error("start application error：{}", ExceptionUtils.getStackTrace(e));
            System.exit(1);
        }
        log.info("infinity消费应用启动成功^^^started");
    }

}
