package com.zwq.infinity.baseplugin;

import com.zwq.infinity.render.FreeMarkerRender;
import com.zwq.infinity.render.TemplateRender;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
public abstract class BaseOutput extends Base {
     private final List<TemplateRender> renders;
    private final List<BaseMetric> metrics;

    public BaseOutput(Map<String, Object> config) {
        this(config, new ArrayList<>());
    }

    public BaseOutput(Map<String, Object> config, List<BaseMetric> metrics) {
        super(config);
        this.metrics = metrics;
        renders = new ArrayList<>();
        if (this.config.containsKey("if")) {
            ((ArrayList<String>) this.config.get("if")).forEach(a-> renders.add(new FreeMarkerRender(a, a)));
        }

        this.prepare();
        this.registerShutdownHookForSelf();
    }
    protected abstract void prepare();

    protected abstract void emit(Map<String, Object> event);

    public void shutdown() {
        log.info("shutdown" + this.getClass().getName());
    }

    /**
     * <p>shut down</p>
     */
    private void registerShutdownHookForSelf() {
        final Object inputClass = this;
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("start to shutdown " + inputClass.getClass().getName());
            shutdown();
        }));
    }


    public void process(Map<String, Object> event) {
        boolean ifSuccess = true;
        if (CollectionUtils.isNotEmpty(renders)) {
            for (TemplateRender render : this.renders) {
                if (!"true".equals(render.render(event))) {
                    ifSuccess = false;
                    break;
                }
            }
        }
        if (ifSuccess) {
            this.emit(event);
        }
    }

    public List<BaseMetric> getMetrics() {
        return metrics;
    }
}
