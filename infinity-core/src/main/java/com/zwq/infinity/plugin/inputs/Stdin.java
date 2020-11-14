package com.zwq.infinity.plugin.inputs;


import com.zwq.infinity.baseplugin.BaseInput;
import com.zwq.infinity.baseplugin.BaseMetric;
import com.zwq.infinity.util.NetworkUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;

@Slf4j
public class Stdin extends BaseInput {
    private Boolean needHostname;
    private String hostname;

    public Stdin(Map<String, Object> config) {
        super(config);
    }

    public Stdin(Map<String, Object> config, List<BaseMetric> metrics) {
        super(config, metrics);
    }

    @Override
    protected void prepare() {
        this.needHostname = (Boolean) config.getOrDefault("needHostname", Boolean.FALSE);
        if (this.needHostname) {
            this.hostname = NetworkUtil.getHostName();
        }
    }

    @Override
    protected Map<String, Object> preprocess(Map<String, Object> event) {
        if (this.needHostname) {
            event.put("hostname", this.hostname);
        }
        return event;
    }

    @Override
    public void shutdown() {
        log.info("Stdin Input shutdown... ");
    }

    @Override
    public void emit() {
        BufferedReader br = null;
        try {
            br = new BufferedReader(new InputStreamReader(System.in));
            String input;
            while ((input = br.readLine()) != null) {
                this.process(input);
            }
        } catch (IOException io) {
            log.info("Stdin loop got exception", io);
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    log.error("Error:{}", ExceptionUtils.getStackTrace(e));
                }
            }
        }
    }
}

