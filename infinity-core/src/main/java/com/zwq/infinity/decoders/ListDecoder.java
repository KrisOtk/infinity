package com.zwq.infinity.decoders;

import com.zwq.infinity.util.InfinityJsonUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class ListDecoder implements Decode {

    @Override
    public Map<String, Object> decode(final String message) {
        List<Map> parse = InfinityJsonUtil.parse2List(message, Map.class);
        Map<String, Object> map = new HashMap<>();
        map.put("list", parse);
        return map;
    }
}
