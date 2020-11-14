package com.zwq.infinity.render;

import com.zwq.infinity.util.Constants;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;

@Slf4j
public class FieldRender implements TemplateRender {
    private final ArrayList<String> fields;

    FieldRender(String template) {
        Matcher matcher = Constants.MULTI_LEVEL_PATTERN.matcher(template);
        this.fields = new ArrayList<>();
        while (matcher.find()) {
            String group = matcher.group();
            this.fields.add(group.substring(1, group.length() - 1));
        }
    }

    @Override
    public Object render(Map<String, Object> event) {
        if (this.fields.size() == 0) {
            return null;
        }

        Object current = event;
        try {
            for (String field : this.fields) {
                if (List.class.isAssignableFrom(current.getClass())) {
                    int i = Integer.parseInt(field);
                    current = ((List) current).get(i);
                } else if (Map.class.isAssignableFrom(current.getClass())) {
                    current = ((Map) current).get(field);
                } else {
                    log.info("render error: current object is not list or map");
                    return null;
                }
            }
            return current;
        } catch (Exception e) {
            log.info("render error: " + e.toString());
            return null;
        }
    }
}
