package com.zwq.infinity.baseplugin;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

@Slf4j
public class InfinityBuilder {
    private static final String INPUT_BASE_PACKAGE = "com.zwq.infinity.plugin.inputs.";
    private static final String FILTER_BASE_PACKAGE = "com.zwq.infinity.plugin.filters.";
    private static final String OUTPUT_BASE_PACKAGE = "com.zwq.infinity.plugin.outputs.";
    private static final String Mertic_BASE_PACKAGE = "com.zwq.infinity.plugin.metrics.";

    private final List<Map<String, Map<String, Object>>> inputConfigs;
    private final List<Map<String, Map<String, Object>>> filterConfigs;
    private final List<Map<String, Map<String, Object>>> outputConfigs;
    private final List<Map<String, Map<String, Object>>> metrics;

    public InfinityBuilder(List<Map<String, Map<String, Object>>> inputConfigs, List<Map<String, Map<String, Object>>> filterConfigs, List<Map<String, Map<String, Object>>> outputConfigs, List<Map<String, Map<String, Object>>> metrics) {
        this.inputConfigs = inputConfigs;
        this.filterConfigs = filterConfigs;
        this.outputConfigs = outputConfigs;
        this.metrics = metrics;
    }

    /**
     * <p>构造input</p>
     *
     * @param metrics
     * @return
     */
    private List<BaseInput> buildInputs(List<BaseMetric> metrics) {
        List<BaseInput> inputs = new ArrayList<>();
        inputConfigs.forEach(input ->
                input.forEach((inputType, inputConfig) -> {
                    log.info("begin to build input " + inputType);
                    String className = INPUT_BASE_PACKAGE + inputType;
                    try {
                        Class<?> inputClass = Class.forName(className);
                        BaseInput inputInstance;
                        if (CollectionUtils.isNotEmpty(metrics)) {
                            Constructor<?> constructor = inputClass.getConstructor(Map.class, List.class);
                            inputInstance = (BaseInput) constructor.newInstance(inputConfig, metrics);
                        } else {
                            Constructor<?> constructor = inputClass.getConstructor(Map.class);
                            inputInstance = (BaseInput) constructor.newInstance(inputConfig);
                        }
                        log.info("build input " + inputType + " done");
                        inputs.add(inputInstance);
                    } catch (Exception e) {
                        if (e instanceof ClassNotFoundException) {
                            log.info("input^^^class not found:{}", e.getMessage());
                        } else {
                            log.info("build input error:{}", ExceptionUtils.getStackTrace(e));
                        }
                        System.exit(1);
                    }
                }));
        return inputs;
    }

    /**
     * <p>构造filter</p>
     *
     * @return filter列表
     */
    private List<BaseFilter> buildFilters(List<BaseMetric> metrics) {
        List<BaseFilter> filterProcessors = new ArrayList<>();
        if (filterConfigs != null) {
            filterConfigs.forEach((filterMap ->
                    filterMap.forEach((filterType, filterConfig) -> {
                        log.info("begin to build filter " + filterType);
                        String className = FILTER_BASE_PACKAGE + filterType;
                        try {
                            if (CollectionUtils.isNotEmpty(metrics)) {
                                Class<?> filterClass = Class.forName(className);
                                Constructor<?> ctor = filterClass.getConstructor(Map.class, List.class);
                                log.info("build filter " + filterType + " done");
                                filterProcessors.add((BaseFilter) ctor.newInstance(filterConfig, metrics));
                            } else {
                                Class<?> filterClass = Class.forName(className);
                                Constructor<?> ctor = filterClass.getConstructor(Map.class);
                                log.info("build filter " + filterType + " done");
                                filterProcessors.add((BaseFilter) ctor.newInstance(filterConfig));
                            }
                        } catch (Exception e) {
                            if (e instanceof ClassNotFoundException) {
                                log.info("filter^^^class not found:{}", e.getMessage());
                            } else {
                                log.info("build filter  error:{}", ExceptionUtils.getStackTrace(e));
                            }
                            System.exit(1);
                        }

                    })));
        }

        return filterProcessors;
    }


    private List<BaseOutput> buildOutputs(List<BaseMetric> metrics) {
        List<BaseOutput> outputProcessors = new ArrayList<>();
        if (outputConfigs != null) {
            outputConfigs.forEach(outputMap ->
                    outputMap.forEach((outputType, outputConfig) -> {
                        log.info("begin to build output " + outputType);
                        String className = OUTPUT_BASE_PACKAGE + outputType;
                        try {
                            if (CollectionUtils.isNotEmpty(metrics)) {
                                Class<?> outputClass = Class.forName(className);
                                Constructor<?> ctor = outputClass.getConstructor(Map.class, List.class);
                                log.info("build output {} done", outputType);
                                outputProcessors.add((BaseOutput) ctor.newInstance(outputConfig, metrics));
                            } else {
                                Class<?> outputClass = Class.forName(className);
                                Constructor<?> ctor = outputClass.getConstructor(Map.class);
                                log.info("build output {} done", outputType);
                                outputProcessors.add((BaseOutput) ctor.newInstance(outputConfig));
                            }
                        } catch (Exception e) {
                            if (e instanceof ClassNotFoundException) {
                                log.info("output^^^class not found:{}", ExceptionUtils.getStackTrace(e));
                            } else {
                                log.info("build output error:{}", ExceptionUtils.getStackTrace(e));
                            }
                            System.exit(1);
                        }
                    }));
        } else {
            log.info("Error: At least One output should be set.");
            System.exit(1);
        }

        return outputProcessors;
    }


    public List<BaseInput> build() {
        List<BaseMetric> metrics = this.buildMetrics();
        List<BaseInput> inputs = this.buildInputs(metrics);
        List<BaseFilter> filters = this.buildFilters(metrics);
        List<BaseOutput> outputs = this.buildOutputs(metrics);

        //流程 input --filter1--filter2--...filter n...--output
        if (filters.size() > 0) {
            //有filter 使用filter
            inputs.forEach(a -> a.nextFilter = filters.get(0));
        } else {
            //没有就直接到output
            inputs.forEach(a -> a.outputs.addAll(outputs));
        }
        int size = filters.size();
        IntStream.range(0, size).forEach(i -> {
            if (i < size - 1) {
                filters.get(i).nextFilter = filters.get(i + 1);
            } else if (i == size - 1) {
                filters.get(i).outputs.addAll(outputs);
            }
        });
        return inputs;
    }

    private List<BaseMetric> buildMetrics() {
        List<BaseMetric> baseMetrics = new ArrayList<>();
        if (metrics != null) {
            metrics.forEach((metricMap ->
                    metricMap.forEach((type, config) -> {
                        log.info("begin to build metric " + type);
                        String className = Mertic_BASE_PACKAGE + type;
                        try {
                            Class<?> metricClass = Class.forName(className);
                            Constructor<?> ctor = metricClass.getConstructor(Map.class);
                            log.info("build metric " + type + " done");
                            baseMetrics.add((BaseMetric) ctor.newInstance(config));
                        } catch (Exception e) {
                            if (e instanceof ClassNotFoundException) {
                                log.info("metric^^^class not found:{}", e.getMessage());
                            } else {
                                log.info("build metric error:{}", ExceptionUtils.getStackTrace(e));
                            }
                            System.exit(1);
                        }
                    })));
        }
        return baseMetrics;
    }
}
