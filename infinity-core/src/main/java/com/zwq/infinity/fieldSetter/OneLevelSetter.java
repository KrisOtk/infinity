package com.zwq.infinity.fieldSetter;

import java.util.Map;


public class OneLevelSetter implements FieldSetter {
    private final String field;

    public OneLevelSetter(String field) {
        this.field = field;
    }

    @Override
    public void setField(Map<String, Object> event, String field, Object value) {
        event.put(field, value);
    }

    @Override
    public void setField(Map<String, Object> event, Object value) {
        event.put(this.field, value);
    }
}
