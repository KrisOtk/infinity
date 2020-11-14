package com.zwq.infinity.fieldDeleter;

import com.zwq.infinity.util.Constants;

import java.util.ArrayList;
import java.util.Map;
import java.util.regex.Matcher;

/**
 * <p>用于去除子属性</p>
 */
public class MultiLevelDeleter implements FieldDeleter {
    private final ArrayList<String> fields;

    MultiLevelDeleter(String template) {
        Matcher m = Constants.MULTI_LEVEL_PATTERN.matcher(template);
        this.fields = new ArrayList<>();
        while (m.find()) {
            String a = m.group();
            this.fields.add(a.substring(1, a.length() - 1));
        }
    }


    @Override
    public Map<String, Object> delete(Map<String, Object> event) {
        if (fields.size() == 0) {
            return event;
        }

        //值传递、引用传递又开始混乱了...
        Map<String, Object> current = event;
        for (int i = 0; i < fields.size() - 1; i++) {
            String field = fields.get(i);
            if (current.containsKey(field)) {
                Object t = current.get(field);
                if (!Map.class.isAssignableFrom(t.getClass())) {
                    return event;
                }
                current = (Map) t;
            } else {
                return event;
            }
        }
        current.remove(fields.get(fields.size() - 1));
        return event;
    }
    }
