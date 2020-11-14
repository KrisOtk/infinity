package com.zwq.infinity.fieldSetter;

import com.zwq.infinity.util.Constants;

import java.util.Map;



public interface FieldSetter {
    void setField(Map<String, Object> event, String field, Object value);

    void setField(Map<String, Object> event, Object value);

    static FieldSetter getFieldSetter(String field) {
        if (Constants.MULTI_LEVEL_PATTERN.matcher(field).matches()) {
            return new MultiLevelSetter(field);
        }
        return new OneLevelSetter(field);
    }
}
