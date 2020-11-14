package com.zwq.infinity.baseplugin;


import lombok.extern.slf4j.Slf4j;

import java.util.Map;

@Slf4j
public abstract class BaseMetric extends Base {
    /**
     * <p>基础构造方法</p>
     * <p>builder通过反射调用构造方法进行实例化</p>
     *
     * @param config
     */
    public BaseMetric(Map<String, Object> config) {
        super(config);
    }
}
