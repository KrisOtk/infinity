package com.zwq.infinity.render;

import com.zwq.infinity.util.Constants;

import java.util.Map;


/**
 * @author kris
 * 模板 用来根据模板规则对obj进行处理
 */
public interface TemplateRender {

    Object render(Map<String, Object> event);

    static TemplateRender getRender(Object template) {
        if (!String.class.isAssignableFrom(template.getClass())) {
            return new DirectRender(template);
        }

        if (Constants.MULTI_LEVEL_PATTERN.matcher((String) template).matches()) {
            return new FieldRender((String) template);
        }

        return new FreeMarkerRender((String) template, (String) template);

    }

    static TemplateRender getRender(String template, boolean ignoreOneLevelRender) {
        if (ignoreOneLevelRender) {
            return getRender(template);
        }
        if (Constants.MULTI_LEVEL_PATTERN.matcher(template).matches()) {
            return new FieldRender(template);
        }

        if (template.contains("$")) {
            return new FreeMarkerRender(template, template);
        }
        return new OneLevelRender(template);
    }

    /**
     * 针对时间的
     * @param template
     * @param timezone
     * @return
     */
    static TemplateRender getRender(String template, String timezone) {
        if (Constants.INDEX_PATTERN.matcher(template).find()) {
            return new DateFormatter(template, timezone);
        }
        return getRender(template);
    }
}