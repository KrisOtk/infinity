package com.zwq.infinity.fieldDeleter;

import com.zwq.infinity.util.Constants;

import java.util.Map;


public interface FieldDeleter {
    Map<String, Object> delete(Map<String, Object> event);

    static FieldDeleter getFieldDeleter(String field) {
        if (Constants.MULTI_LEVEL_PATTERN.matcher(field).matches()) {
            return new MultiLevelDeleter(field);
        }
        return new OneLevelDeleter(field);
    }
}
