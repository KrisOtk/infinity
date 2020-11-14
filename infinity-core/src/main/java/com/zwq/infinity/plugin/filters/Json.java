package com.zwq.infinity.plugin.filters;

import com.zwq.infinity.baseplugin.BaseFilter;
import com.zwq.infinity.baseplugin.BaseMetric;
import com.zwq.infinity.fieldSetter.FieldSetter;
import com.zwq.infinity.render.TemplateRender;
import com.zwq.infinity.util.InfinityJsonUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;

@Slf4j
public class Json extends BaseFilter {

    public Json(Map<String, Object> config) {
        super(config);
    }

    public Json(Map<String, Object> config, List<BaseMetric> metrics) {
        super(config, metrics);
    }

    private String field;
    private TemplateRender templateRender;
    private FieldSetter fieldSetter;

    @Override
    protected void prepare() {
        if (!config.containsKey("field")) {
            log.info("no field configured in Json filter");
            System.exit(1);
        }
        this.field = (String) config.get("field");
        String target = (String) config.get("target");
        if (StringUtils.isBlank(target)) {
            this.fieldSetter = null;
        } else {
            this.fieldSetter = FieldSetter.getFieldSetter(target);
        }

        if (config.containsKey("tag_on_failure")) {
            this.tagOnFailure = (String) config.get("tag_on_failure");
        } else {
            this.tagOnFailure = "jsonfail";
        }

        this.templateRender = TemplateRender.getRender(field, false);
    }

    @Override
    protected Map<String, Object> filter(Map<String, Object> event) {
        Object obj = null;
        boolean success = false;

        Object object = this.templateRender.render(event);
        if (object != null) {
            try {
                obj = InfinityJsonUtil.parse((String) object, Object.class);
                success = true;
            } catch (Exception e) {
                log.info("failed to parse field to json^^^field:{}^^^message:{} ", this.field, object);
            }
        }

        if (obj != null) {
            if (this.fieldSetter == null) {
                try {
                    event.putAll((Map) obj);
                } catch (Exception e) {
                    log.info(this.field + " is not a map, you should set a target to save it");
                    success = false;
                }
            } else {
                this.fieldSetter.setField(event, obj);
            }
        }

        this.postProcess(event, success);
        return event;
    }
}
