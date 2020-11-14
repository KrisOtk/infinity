package com.zwq.infinity.render;

import lombok.extern.slf4j.Slf4j;

import java.util.Map;

@Slf4j
public class OneLevelRender implements TemplateRender {
    private final String field;

    OneLevelRender(String field) {
        this.field = field;
    }

    @Override
    public Object render(Map<String, Object> event) {
        return event.get(field);
    }
}
