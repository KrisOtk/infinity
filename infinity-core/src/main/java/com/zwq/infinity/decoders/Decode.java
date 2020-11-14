package com.zwq.infinity.decoders;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

public interface Decode {
    /**
     * <p>对消息进行前置处理 比如添加上一些必要的字段等</p>
     * @param message
     * @return
     */
    Map<String, Object> decode(String message);

    /**
     * <p>默认处理方式 加个时间戳返回了...</p>
     * @param message
     * @return Map<String, Object>
     */
    default Map<String, Object> createDefaultEvent(String message) {
        return new HashMap<String, Object>() {
            private static final long serialVersionUID = -7036294667175860989L;
            {
                put("message", message);
                put("@timestamp", DateTime.now(DateTimeZone.forTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))));
            }
        };
    }
}
