package com.zwq.infinity.baseplugin;

import com.zwq.infinity.decoders.Decode;
import com.zwq.infinity.decoders.JsonDecoder;
import com.zwq.infinity.decoders.ListDecoder;
import com.zwq.infinity.decoders.PlainDecoder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
public abstract class BaseInput extends Base {
    private Decode decoder;
    BaseFilter nextFilter;
    List<BaseOutput> outputs = new ArrayList<>();
    List<BaseMetric> metrics = new ArrayList<>();


    public BaseInput(Map<String, Object> config) {
        super(config);
        this.createDecoder();
        this.prepare();
        this.registerShutdownHookForSelf();
    }

    public BaseInput(Map<String, Object> config, List<BaseMetric> metrics) {
        super(config);
        this.createDecoder();
        this.prepare();
        this.metrics = metrics;
    }

    /**
     * eg: 根据config去构造子类中的配置信息
     */
    protected abstract void prepare();

    /**
     * 执行,获取数据并处理(后置的filter和output)
     */
    public abstract void emit();


    /**
     * <p>前置处理</p>
     *
     * @param event
     * @return
     */
    protected Map<String, Object> preprocess(Map<String, Object> event) {
        return event;
    }

    /**
     * <p>后置处理</p>
     *
     * @param event
     * @return
     */
    protected Map<String, Object> postprocess(Map<String, Object> event) {
        return event;
    }


    /**
     * <p>对输入消息进行处理的核心方法 filter和output</p>
     *
     * @param message
     */
    public void process(String message) {
        try {
            Map<String, Object> event = this.decoder.decode(message);
            if (this.config.get("codec").equals("list")) {
                List<Map<String, Object>> list = (ArrayList<Map<String, Object>>) event.get("list");
                list.forEach(item1 -> {
                    if (this.config.containsKey("type")) {
                        item1.put("type", this.config.get("type"));
                    }
                    item1 = this.preprocess(item1);
                    if (this.nextFilter != null) {
                        this.nextFilter.process(item1);
                    } else {
                        Map<String, Object> finalEvent = item1;
                        this.outputs.forEach(output -> output.process(finalEvent));
                    }
                });
            } else {
                if (this.config.containsKey("type")) {
                    event.put("type", this.config.get("type"));
                }
                event = this.preprocess(event);
                if (this.nextFilter != null) {
                    this.nextFilter.process(event);
                } else {
                    Map<String, Object> finalEvent = event;
                    this.outputs.forEach(output -> output.process(finalEvent));
                }
            }
        } catch (Throwable e) {
            log.info("process event failed:{},error:{}", message, ExceptionUtils.getStackTrace(e));
        }
    }


    /**
     * <p>消息处理器 对消息进行格式化</p>
     */
    private void createDecoder() {
        String codec = (String) this.config.get("codec");
        if ("plain".equalsIgnoreCase(codec)) {
            decoder = new PlainDecoder();
        } else if ("list".equalsIgnoreCase(codec)) {
            decoder = new ListDecoder();
        } else {
            decoder = new JsonDecoder();
        }
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

    /**
     * <p>优雅停机...</p>
     */
    protected abstract void shutdown();


    public List<BaseMetric> getMetrics() {
        return metrics;
    }
}
