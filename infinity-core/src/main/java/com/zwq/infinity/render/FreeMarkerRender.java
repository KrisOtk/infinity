package com.zwq.infinity.render;

import com.alibaba.fastjson.JSON;
import freemarker.template.Configuration;
import freemarker.template.Template;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Map;

@Slf4j
public class FreeMarkerRender implements TemplateRender {
    private Template template;

    public FreeMarkerRender(String template, String templateName) {
        Configuration cfg = new Configuration(Configuration.VERSION_2_3_22);
        try {
            this.template = new Template(templateName, template, cfg);
        } catch (IOException e) {
            log.debug("FreeMarkerRender init template error:{}", ExceptionUtils.getStackTrace(e));
            System.exit(1);
        }
    }

    @Override
    public Object render(Map<String, Object> event) {
        StringWriter sw = new StringWriter();
        try {
            template.process(event, sw);
        } catch (Exception e) {
            log.debug("freemarker parse error:{}^^^event:{}", ExceptionUtils.getStackTrace(e), JSON.toJSON(event));
        } finally {
            try {
                sw.close();
            } catch (IOException e) {
                log.debug("StringWriter close error:{}^^^event:{}", ExceptionUtils.getStackTrace(e), JSON.toJSON(event));
            }
        }
        return sw.toString();
    }
}
