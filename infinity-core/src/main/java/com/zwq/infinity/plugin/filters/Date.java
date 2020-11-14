package com.zwq.infinity.plugin.filters;

import com.zwq.infinity.baseplugin.BaseFilter;
import com.zwq.infinity.baseplugin.BaseMetric;
import com.zwq.infinity.fieldSetter.FieldSetter;
import com.zwq.infinity.plugin.filters.dateparser.*;
import com.zwq.infinity.plugin.filters.dateparser.*;
import com.zwq.infinity.render.TemplateRender;
import com.zwq.infinity.util.InfinityJsonUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author kris
 * 处理时间的filter,常用格式如下
 * <p>
 * - "YYYY-MM-dd HH:mm:ss.SSS"
 * - "YYYY-MM-dd'T'HH:mm:ss.SSS"
 * - "YYYY-MM-dd'T'HH:mm:ss.SSSZ"
 * - "YYYY-MM-dd'T'HH:mm:ssZ"
 * - "YYYY-MM-dd HH:mm:ss"
 * </p>
 */
@Slf4j
public class Date extends BaseFilter {
    private TemplateRender templateRender;
    private FieldSetter fiedlSetter;
    private List<DateParser> parsers;

    public Date(Map<String, Object> config) {
        super(config);
    }

    public Date(Map<String, Object> config, List<BaseMetric> metrics) {
        super(config, metrics);
    }

    @Override
    protected void prepare() {
        String src = "logtime";
        if (config.containsKey("src")) {
            src = (String) config.get("src");
        }
        this.templateRender = TemplateRender.getRender(src, false);

        String target = "@timestamp";
        if (config.containsKey("target")) {
            target = (String) config.get("target");
        }
        this.fiedlSetter = FieldSetter.getFieldSetter(target);

        if (config.containsKey("tag_on_failure")) {
            this.tagOnFailure = (String) config.get("tag_on_failure");
        } else {
            this.tagOnFailure = "datefail";
        }

        this.parsers = new ArrayList<>();
        List<String> formats = (ArrayList<String>) config.get("formats");
        formats.forEach(format -> {
            if ("ISO8601".equalsIgnoreCase(format)) {
                parsers.add(new ISODateParser((String) config.get("timezone")));
            } else if ("UNIX".equalsIgnoreCase(format)) {
                parsers.add(new UnixParser());
            } else if ("UNIX_MS".equalsIgnoreCase(format)) {
                parsers.add(new UnixMSParser());
            } else {
                //正常用这种即可
                String locale = (String) config.get("locale");
                String timezone = (String) config.getOrDefault("timezone", "Asia/Shanghai");
                parsers.add(new FormatParser(format, timezone, locale));
            }
        });
    }

    @Override
    protected Map<String, Object> filter(Map<String, Object> event) {
        Object inputObj = this.templateRender.render(event);
        if (inputObj == null) {
            return event;
        }

        String dateString = inputObj.toString();
        boolean success = false;
        for (DateParser parser : this.parsers) {
            try {
                this.fiedlSetter.setField(event, parser.parse(dateString));
                success = true;
                break;
            } catch (Throwable e) {
                //这里最好不要打debug日志
                log.debug("parser date failed,try next,date:{}", dateString);
            }
        }
        postProcess(event, success);
        return event;
    }

}
