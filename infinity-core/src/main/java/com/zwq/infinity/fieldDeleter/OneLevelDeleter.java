package com.zwq.infinity.fieldDeleter;

import java.util.Map;


public class OneLevelDeleter implements FieldDeleter {
    private final String field;

    public OneLevelDeleter(String field) {
        this.field = field;
    }

    @Override
    public Map<String, Object> delete(Map<String, Object> event) {
        event.remove(field);
        return event;
    }
}
