package com.zwq.infinity.baseplugin;


import com.zwq.infinity.fieldDeleter.FieldDeleter;
import com.zwq.infinity.fieldSetter.FieldSetter;
import com.zwq.infinity.render.FreeMarkerRender;
import com.zwq.infinity.render.TemplateRender;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.*;

@Slf4j
public abstract class BaseFilter extends Base {

    protected String tagOnFailure;
    protected List<FieldDeleter> removeFields;
    protected Map<FieldSetter, TemplateRender> addFields;
    private List<TemplateRender> IF;
    public boolean processExtraEventsFunc;
    protected BaseFilter nextFilter;
    protected List<BaseOutput> outputs;
    private List<BaseMetric> metrics;

    /**
     * <p>基础构造方法</p>
     * <p>builder通过反射调用构造方法进行实例化</p>
     *
     * @param config
     */
    public BaseFilter(Map<String, Object> config) {
        super(config);
        this.nextFilter = null;
        this.outputs = new ArrayList<>();
        if (this.config.containsKey("if")) {
            IF = new ArrayList<>();
            ((ArrayList<String>) this.config.get("if")).forEach(state -> {
                if (StringUtils.isNotBlank(state)) {
                    IF.add(new FreeMarkerRender(state, state));
                }
            });
        }

        if (config.containsKey("tag_on_failure") && StringUtils.isNotBlank((String) config.get("tag_on_failure"))) {
            this.tagOnFailure = (String) config.get("tag_on_failure");
        }
        removeFields = new ArrayList<>();
        if (config.containsKey("remove_fields")) {
            ((ArrayList<String>) config.get("remove_fields")).forEach(field -> {
                if (StringUtils.isNotBlank(field)) {
                    this.removeFields.add(FieldDeleter.getFieldDeleter(field));
                }
            });
        }
        addFields = new HashMap<>();
        if (config.containsKey("add_fields")) {
            ((Map<String, String>) config.get("add_fields")).forEach((field, value) -> this.addFields.put(FieldSetter.getFieldSetter(field), TemplateRender.getRender(value)));
        }
        this.prepare();
    }


    public BaseFilter(Map<String, Object> config, List<BaseMetric> metrics) {
        this(config);
        this.metrics = metrics;
    }

    /**
     * <p>前置准备 个性化配置</p>
     */
    protected void prepare() {
    }

    private boolean needProcess(Map<String, Object> event) {
        if (CollectionUtils.isNotEmpty(this.IF)) {
            for (TemplateRender render : this.IF) {
                if (!"true".equals(render.render(event))) {
                    return false;
                }
            }
        }
        return true;
    }

    public Map<String, Object> process(Map<String, Object> event) {
        if (event == null) {
            return null;
        }

        if (this.needProcess(event)) {
            event = this.filter(event);
        }

        if (event == null) {
            return null;
        }

        if (this.nextFilter != null) {
            event = this.nextFilter.process(event);
        } else {
            //添加额外的timestamp字段 处理有时候没有timestamp字段的问题
            if (event.get("@timestamp") == null) {
                event.put("@timestamp", DateTime.now(DateTimeZone.forTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))));
            }
            for (BaseOutput output : this.outputs) {
                output.process(event);
            }
        }
        return event;
    }

    public void processExtraEvents(Stack<Map<String, Object>> to_st) {
        this.filterExtraEvents(to_st);
    }

    protected Map<String, Object> filter(Map<String, Object> event) {
        return event;
    }

    protected void filterExtraEvents(Stack<Map<String, Object>> to_stt) {
    }

    protected void postProcess(Map<String, Object> event, boolean ifSuccess) {
        if (!ifSuccess) {
            if (StringUtils.isBlank(tagOnFailure)) {
                return;
            }
            if (!event.containsKey("tags")) {
                event.put("tags", new ArrayList<>(Collections.singletonList(this.tagOnFailure)));
            } else {
                Object tags = event.get("tags");
                if (tags.getClass() == ArrayList.class && ((ArrayList) tags).indexOf(this.tagOnFailure) == -1) {
                    ((ArrayList) tags).add(this.tagOnFailure);
                }
            }
        } else {
            if (CollectionUtils.isNotEmpty(this.removeFields)) {
                this.removeFields.forEach(fieldDeleter -> fieldDeleter.delete(event));
            }
            if (this.addFields != null) {
                this.addFields.forEach((fieldSetter, templateRender) -> fieldSetter.setField(event, templateRender.render(event)));
            }
        }
    }

    public List<BaseMetric> getMetrics() {
        return metrics;
    }
}
