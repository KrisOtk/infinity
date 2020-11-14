package com.zwq.infinity.plugin.filters;

import com.zwq.infinity.baseplugin.BaseFilter;
import com.zwq.infinity.baseplugin.BaseMetric;
import com.zwq.infinity.fieldSetter.FieldSetter;
import com.zwq.infinity.render.TemplateRender;
import io.vavr.Tuple2;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 字段值小写 eg: name:ZWQ    -->  name:zwq
 */
@Slf4j
public class Lowercase extends BaseFilter {
    public Lowercase(Map config) {
        super(config);
    }

    public Lowercase(Map config, List<BaseMetric> metrics) {
        super(config, metrics);
    }

    private ArrayList<Tuple2> fields;

    @Override
    protected void prepare() {
        this.fields = new ArrayList<>();
        ArrayList<String> fields = (ArrayList<String>) config.get("fields");
        fields.forEach(field-> this.fields.add(new Tuple2<>(FieldSetter.getFieldSetter(field), TemplateRender.getRender(field,false))));
    }

    @Override
    protected Map filter(Map event) {
        fields.forEach(tuple2 -> {
            Object input = ((TemplateRender) tuple2._2()).render(event);
            if (input != null && String.class.isAssignableFrom(input.getClass())) {
                ((FieldSetter) tuple2._1()).setField(event, ((String) input).toLowerCase());
            }
        });
        return event;
    }
}
