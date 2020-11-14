package com.zwq.infinity.plugin.filters;

import com.zwq.infinity.baseplugin.BaseFilter;
import com.zwq.infinity.baseplugin.BaseMetric;
import com.zwq.infinity.fieldSetter.FieldSetter;
import com.zwq.infinity.render.TemplateRender;
import io.vavr.Tuple3;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author kris
 * 将string类型转换为指定的类型(targetType),targetType必须有单个string的构造方法 否则丢弃要转换的字段
 * usage：
 * - Convert:
 * list:
 * - {src: 'apicosttime',target: 'apicosttime',targetType: 'java.lang.Long'}
 */
@Slf4j
public class Convert extends BaseFilter {
    private List<Tuple3<TemplateRender, FieldSetter, String>> convertList;

    public Convert(Map<String, Object> config) {
        super(config);
    }
    public Convert(Map<String, Object> config, List<BaseMetric> metrics) {
        super(config, metrics);
    }

    @Override
    protected void prepare() {
        List<Map<String, Object>> list = (List<Map<String, Object>>) config.get("list");
        if (CollectionUtils.isEmpty(list)) {
            log.info("convert should specified source list");
            System.exit(1);
        }

        List<Tuple3<TemplateRender, FieldSetter, String>> convertList = new ArrayList<>();
        list.forEach(map -> {
            if (!map.containsKey("src") || !map.containsKey("target") || !map.containsKey("targetType")) {
                log.info("convert item should specified src and target and targetType");
                System.exit(1);
            }
            String src = (String) map.get("src");
            String target = (String) map.get("target");
            String targetType = (String) map.get("targetType");
            TemplateRender render = TemplateRender.getRender(src, false);
            FieldSetter fieldSetter = FieldSetter.getFieldSetter(target);
            convertList.add(new Tuple3<>(render, fieldSetter, targetType));
        });
        this.convertList = convertList;
    }

    @Override
    protected Map<String, Object> filter(Map<String, Object> event) {
        this.convertList.forEach(tuple -> {
            Object inputObj = tuple._1().render(event);
            if (inputObj != null) {
                try {
                    Class<?> filterClass = Class.forName(tuple._3());
                    Object o = filterClass.getConstructor(String.class).newInstance(inputObj.toString());
                    tuple._2().setField(event, o);
                } catch (Exception e) {
                    tuple._2().setField(event, null);
                    log.info("字段转换失败:{}", ExceptionUtils.getStackTrace(e));
                }
            }
        });
        return event;
    }

}
