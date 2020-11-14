package com.zwq.infinity.decoders;

import com.zwq.infinity.util.InfinityJsonUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

@Slf4j
public class JsonDecoder implements Decode {

    @Override
    public Map<String, Object> decode(final String message) {
            return InfinityJsonUtil.parse(message, Map.class);
    }
}
