package com.zwq.infinity.plugin.filters;

import com.zwq.infinity.baseplugin.BaseFilter;
import com.zwq.infinity.baseplugin.BaseMetric;
import com.zwq.infinity.util.Utils;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Stack;

/**
 * 组合型filter 暂未使用
 */
@Slf4j
public class Filters extends BaseFilter {
    private List<BaseFilter> filterProcessors;
    private Map<String, Object> event;


    public Filters(Map<String, Object> config) {
        super(config);
    }

    public Filters(Map<String, Object> config, List<BaseMetric> metrics) {
        super(config, metrics);
    }

    @Override
    protected void prepare() {
        List<Map<String, Object>> filters = (ArrayList<Map<String, Object>>) config.get("filters");
        this.filterProcessors = Utils.createFilterProcessors(filters);
        for (BaseFilter filterProcessor : filterProcessors) {
            if (filterProcessor.processExtraEventsFunc) {
                this.processExtraEventsFunc = true;
                break;
            }
        }
    }

    @Override
    protected Map<String, Object> filter(Map<String, Object> event) {
        if (this.processExtraEventsFunc) {
            //will prcess the event in filterExtraEvents
            this.event = event;
            return event;
        }
        if (this.filterProcessors != null) {
            for (BaseFilter bf : filterProcessors) {
                if (event == null) {
                    break;
                }
                event = bf.process(event);
            }
        }
        return event;
    }

    @Override
    protected void filterExtraEvents(Stack<Map<String, Object>> to_st) {
        Stack<Map<String, Object>> from_st = new Stack<>();
        from_st.push(event);
        for (BaseFilter bf : filterProcessors) {
            while (!from_st.empty()) {
                Map<String, Object> rst = bf.process(from_st.pop());
                if (rst != null) {
                    to_st.push(rst);
                }
            }
            if (bf.processExtraEventsFunc) {
                bf.processExtraEvents(to_st);
            }
        }
    }
}
