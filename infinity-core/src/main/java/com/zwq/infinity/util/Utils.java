package com.zwq.infinity.util;

import com.zwq.infinity.baseplugin.BaseFilter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;


@Slf4j
public class Utils {
    public static List<BaseFilter> createFilterProcessors(List<Map<String, Object>> filters) {
        List<BaseFilter> filterProcessors = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(filters)) {
            filters.forEach((Map<String, Object> filterMap) -> {
                filterMap.forEach((filterType, value) -> {
                    Map<String, Object> filterConfig = (Map<String, Object>) value;
                    log.info("begin to build filter " + filterType);
                    Class<?> filterClass;
                    Constructor<?> ctor;
                    List<String> classNames = Arrays.asList("com.zwq.infinity.plugin.filters." + filterType, filterType);
                    for (String className : classNames) {
                        try {
                            filterClass = Class.forName(className);
                            ctor = filterClass.getConstructor(Map.class);
                            log.info("build filter " + filterType + " done");
                            filterProcessors.add((BaseFilter) ctor.newInstance(filterConfig));
                            break;
                        } catch (Exception e) {
                            log.error("load class error:{}", ExceptionUtils.getStackTrace(e));
                            System.exit(1);
                        }
                    }
                });
            });
        }

        return filterProcessors;
    }

}
