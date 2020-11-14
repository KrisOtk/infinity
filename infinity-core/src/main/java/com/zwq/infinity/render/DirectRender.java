package com.zwq.infinity.render;

import java.util.Map;

public class DirectRender implements TemplateRender {
    private final Object value;

    DirectRender(Object value) {
        this.value = value;
    }

    @Override
    public Object render(Map<String, Object> event) {
        return this.value;
    }
}
