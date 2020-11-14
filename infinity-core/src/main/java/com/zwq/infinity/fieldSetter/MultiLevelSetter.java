package com.zwq.infinity.fieldSetter;

import com.zwq.infinity.util.Constants;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;

/**
 * @author kris
 */
public class MultiLevelSetter implements FieldSetter {
    private final List<String> fields;

    MultiLevelSetter(String template) {
        Matcher m = Constants.MULTI_LEVEL_PATTERN.matcher(template);
        this.fields = new ArrayList<>();
        while (m.find()) {
            String group = m.group();
            this.fields.add(group.substring(1, group.length() - 1));
        }
    }

    @Override
    public void setField(Map<String, Object> event, String field, Object value) {

    }

    @Override
    public void setField(Map<String, Object> event, Object value) {
        if (this.fields.size() == 0) {
            return;
        }

        Map<String, Object> current = event;
        for (int i = 0; i < this.fields.size() - 1; i++) {
            String field = this.fields.get(i);
            if (current.containsKey(field)) {
                current = (Map) current.get(field);
            } else {
                Map<String, Object> next = new HashMap<>();
                current.put(field, next);
                current = next;
            }
        }
        current.put(this.fields.get(this.fields.size() - 1), value);
    }
}
