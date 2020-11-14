package com.zwq.infinity.plugin.filters;

import com.zwq.infinity.baseplugin.BaseFilter;
import com.zwq.infinity.baseplugin.BaseMetric;
import com.zwq.infinity.fieldDeleter.FieldDeleter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 移除字段
 */
public class Remove extends BaseFilter {
    public Remove(Map<String, Object> config) {
        super(config);
    }
    public Remove(Map<String, Object> config, List<BaseMetric> metrics) {
        super(config, metrics);
    }

    private ArrayList<FieldDeleter> fields;

    @Override
    protected void prepare() {
        this.fields = new ArrayList<>();
        ArrayList<String> fields = (ArrayList<String>) config.get("fields");
        fields.forEach(field -> this.fields.add(FieldDeleter.getFieldDeleter(field)));
    }

    @Override
    protected Map<String, Object> filter(final Map<String, Object> event) {
        this.fields.forEach(fieldDeleter -> fieldDeleter.delete(event));
        return event;
    }
}
