package com.zwq.infinity.plugin.filters;

import com.zwq.infinity.baseplugin.BaseFilter;
import com.zwq.infinity.baseplugin.BaseMetric;
import com.zwq.infinity.render.TemplateRender;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author kris
 * 添加字段的filter
 */
@Slf4j
public class Deprecate extends BaseFilter {
    public Deprecate(Map<String, Object> config) {
        super(config);
    }

    public Deprecate(Map<String, Object> config, List<BaseMetric> metrics) {
        super(config, metrics);
    }

    private List<TemplateRender> templateRenders;


    @Override
    protected void prepare() {
        templateRenders = new ArrayList<>();
        List<String> list = (List<String>) config.get("columns");
        list.forEach(value -> templateRenders.add(TemplateRender.getRender(value)));

    }

    @Override
    protected Map<String, Object> filter(final Map<String, Object> event) {
        boolean aTrue = templateRenders.stream().anyMatch(value -> {
            try {
                String render = (String) value.render(event);
                return render.equals("true");
            } catch (Exception e) {
                log.debug("error:{}", ExceptionUtils.getStackTrace(e));
                return false;
            }
        });
        if (aTrue) {
            return null;
        } else {
            return event;
        }
    }
}
