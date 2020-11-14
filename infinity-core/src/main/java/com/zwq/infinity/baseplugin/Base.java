package com.zwq.infinity.baseplugin;

import lombok.Data;

import java.util.Map;

/**
 * 基类
 */
@Data
public class Base {
    protected Map<String, Object> config;

    Base(Map<String, Object> config) {
        this.config = config;
    }
}
