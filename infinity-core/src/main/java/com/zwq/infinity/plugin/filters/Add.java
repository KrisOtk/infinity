package com.zwq.infinity.plugin.filters;

import com.zwq.infinity.baseplugin.BaseFilter;
import com.zwq.infinity.baseplugin.BaseMetric;
import com.zwq.infinity.fieldSetter.FieldSetter;
import com.zwq.infinity.render.TemplateRender;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author kris
 * 添加字段的filter
 */
@Slf4j
public class Add extends BaseFilter {
    public Add(Map<String, Object> config) {
        super(config);
    }

    public Add(Map<String, Object> config, List<BaseMetric> metrics) {
        super(config, metrics);
    }

    private Map<FieldSetter, TemplateRender> renderMap;


    @Override
    protected void prepare() {
        renderMap = new HashMap<>();
        Map<String, String> fields = (HashMap<String, String>) config.getOrDefault("fields",new HashMap<String, String>());
        fields.forEach((key, value) -> renderMap.put(FieldSetter.getFieldSetter(key), TemplateRender.getRender(value)));
    }

    @Override
    protected Map<String, Object> filter(final Map<String, Object> event) {
        renderMap.forEach((key, value) -> {
            key.setField(event, value.render(event));
        });
        return event;
    }
}
