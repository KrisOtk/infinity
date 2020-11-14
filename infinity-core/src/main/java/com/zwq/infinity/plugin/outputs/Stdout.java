package com.zwq.infinity.plugin.outputs;

import com.zwq.infinity.baseplugin.BaseMetric;
import com.zwq.infinity.baseplugin.BaseOutput;
import com.zwq.infinity.render.TemplateRender;
import com.zwq.infinity.util.InfinityJsonUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;

@Slf4j
public class Stdout extends BaseOutput {
    private TemplateRender formatRender;

    public Stdout(Map<String, Object> config) {
        super(config);
    }

    public Stdout(Map<String, Object> config, List<BaseMetric> metrics) {
        super(config, metrics);
    }

    @Override
    protected void prepare() {
        if (this.config.containsKey("format")) {
            String format = (String) this.config.get("format");
            this.formatRender = TemplateRender.getRender(format);
        }
    }

    @Override
    protected void emit(Map<String, Object> event) {
        if (formatRender != null) {
            Object message = this.formatRender.render(event);
            if (message != null) {
                if (message instanceof String) {
                    log.info((String) message);
                } else {
                    log.info(InfinityJsonUtil.toJSON(message));
                }
            } else {
                log.info("stdout error^^^the message is null");
            }
        } else {
            log.info(InfinityJsonUtil.toJSON(event));
        }

    }
}

