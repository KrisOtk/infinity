package com.zwq.infinity.plugin.outputs;

import com.zwq.infinity.baseplugin.BaseMetric;
import com.zwq.infinity.baseplugin.BaseOutput;
import com.zwq.infinity.plugin.metrics.MicroKafkaMetric;
import com.zwq.infinity.render.TemplateRender;
import com.zwq.infinity.util.InfinityJsonUtil;
import io.micrometer.core.instrument.Counter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.sniff.Sniffer;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Slf4j
public class RestElasticsearch extends BaseOutput {
    private final static int BULKACTION = 20000;
    private final static int BULKSIZE = 15; //MB
    private final static int FLUSHINTERVAL = 15;
    private final static int CONCURRENTREQSIZE = 1;

    private TemplateRender indexRender;
    private BulkProcessor bulkProcessor;
    private BulkProcessor retryBulkProcessor;
    private RestHighLevelClient restClient;
    private TemplateRender indexTypeRender;
    private TemplateRender idRender;
    private TemplateRender routeRender;

    public RestElasticsearch(Map<String, Object> config) {
        super(config);
    }

    public RestElasticsearch(Map<String, Object> config, List<BaseMetric> baseMetrics) {
        super(config, baseMetrics);
    }

    @Override
    protected void prepare() {
        String indexTimezone = config.containsKey("timezone") ? (String) config.get("timezone") : "UTC";
        String index = (String) config.get("index");
        this.indexRender = TemplateRender.getRender(index, indexTimezone);

        String indexType = "_doc";
        if (config.containsKey("index_type") && config.get("index_type") != null) {
            indexType = (String) config.get("index_type");
        }
        this.indexTypeRender = TemplateRender.getRender(indexType);


        if (config.containsKey("document_id") && config.get("document_id") != null) {
            String documentId = (String) config.get("document_id");
            this.idRender = TemplateRender.getRender(documentId);
        }


        if (config.containsKey("route") && config.get("route") != null) {
            String route = (String) config.get("route");
            this.routeRender = TemplateRender.getRender(route);
        }

        List<String> hosts = (ArrayList<String>) config.get("hosts");
        int bulkActions = config.containsKey("bulk_actions") ? (int) config.get("bulk_actions") : BULKACTION;
        int bulkSize = config.containsKey("bulk_size") ? (int) config.get("bulk_size") : BULKSIZE;
        int flushInterval = config.containsKey("flush_interval") ? (int) config.get("flush_interval") : FLUSHINTERVAL;
        int concurrentRequests = config.containsKey("concurrent_requests") ? (int) config.get("concurrent_requests") : CONCURRENTREQSIZE;
        String username = config.containsKey("username") ? (String) config.get("username") : "";
        String password = config.containsKey("password") ? (String) config.get("password") : "";


        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
        //初始化 RestHighLevelClient
        RestClientBuilder restClientBuilder = RestClient
                .builder(hosts.stream()
                        .map(host -> host.split(":")).filter(a -> a.length == 2)
                        .map(ipAndPort -> new HttpHost(ipAndPort[0], Integer.parseInt(ipAndPort[1]), "http"))
                        .toArray(HttpHost[]::new))
                .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
        restClient = new RestHighLevelClient(restClientBuilder);
        Header header = new BasicHeader("Keep-Alive", "timeout=60, max=100");
        Header[] headers = {header};
        restClientBuilder.setDefaultHeaders(headers);
        //是否开启嗅探机制,sniffer输出debug级别的日志
        boolean sniff = !(config.containsKey("sniff") && !((boolean) config.get("sniff")));
        log.info("是否开启节点嗅探机制:{}", sniff);
        if (sniff) {
            Sniffer.builder(restClient.getLowLevelClient()).build();
        }

        //通过配置获取bulk api的回调
        BulkProcessor.Listener bulkListener = getBulkListener(this.getMetrics());

        this.bulkProcessor = BulkProcessor
                .builder((bulkRequest, listener) -> restClient.bulkAsync(bulkRequest, RequestOptions.DEFAULT, listener), bulkListener)
                .setBulkActions(bulkActions)
                .setBulkSize(new ByteSizeValue(bulkSize, ByteSizeUnit.MB))
                .setFlushInterval(TimeValue.timeValueSeconds(flushInterval))
                .setConcurrentRequests(concurrentRequests)//默认是1，表示积累bulk requests和发送bulk是异步的，其数值表示发送bulk的并发线程数，设置为0表示二者同步的
//                .setBackoffPolicy(BackoffPolicy.noBackoff())
                .build();

        RetryRestElasticsearch retryRestElasticsearch = new RetryRestElasticsearch(this.getMetrics());
        retryRestElasticsearch.buildRetryBulkProcessor(config);
        this.retryBulkProcessor = retryRestElasticsearch.getRetryBulkProcessor();
    }


    @Override
    protected void emit(final Map<String, Object> event) {
        String index = (String) this.indexRender.render(event);
        String indexType = (String) indexTypeRender.render(event);
        IndexRequest indexRequest;
        if (this.idRender == null) {
            indexRequest = new IndexRequest(index, indexType).source(event);
        } else {
            String id = (String) idRender.render(event);
            indexRequest = new IndexRequest(index, indexType).id(id).source(event);
        }
        if (this.routeRender != null) {
            indexRequest.routing((String) this.routeRender.render(event));
        }
        this.bulkProcessor.add(indexRequest);
    }

    @Override
    public void shutdown() {
        log.info("flush docs and then shutdown");
        //flush immediately
        this.bulkProcessor.flush();
        // await for some time for rest data from input
        int flushInterval = 10;
        if (config.containsKey("flush_interval")) {
            flushInterval = (int) config.get("flush_interval");
        }
        try {
            this.bulkProcessor.awaitClose(flushInterval, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            log.info("failed to bulk docs before shutdown,exception:{}", ExceptionUtils.getStackTrace(e));
        }
    }

    private BulkProcessor.Listener getBulkListener(List<BaseMetric> baseMetrics) {
        return new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                List<DocWriteRequest<?>> requests = request.requests();
                int toBeTry = 0;
                int totalFailed = 0;
                for (BulkItemResponse item : response.getItems()) {
                    if (item.isFailed()) {
                        switch (item.getFailure().getStatus()) {
                            //前面两种默认去重试 第一个没有break,所以会执行到第一个break的地方
                            case TOO_MANY_REQUESTS:
                            case SERVICE_UNAVAILABLE:
                                toBeTry++;
                                if (retryBulkProcessor != null) {
                                    retryBulkProcessor.add(requests.get(item.getItemId()));
                                } else {
                                    log.info("bulk has failed item which can not retry.message:{},item:{}", item.getFailureMessage(), InfinityJsonUtil.toJSON(requests.get(item.getItemId())));
                                }
                                break;
                            default:
                                //默认不处理了
                                break;
                        }
                        totalFailed++;
                    }
                }

                if (toBeTry > 0) {
                    log.info("bulkFailed^^^bulk done with executionId:{}^^^bulk size:{}^^^{} doc failed,{} need to retry", executionId, request.numberOfActions(), totalFailed, toBeTry);
                    try {
                        Thread.sleep(toBeTry / 2);
                        log.info("sleep " + toBeTry / 2 + "milliseconds after bulk failure");
                    } catch (InterruptedException e) {
                        log.info("after bulk^^^thread sleep failed error,InterruptedException:{}", ExceptionUtils.getStackTrace(e));
                    }
                } else if (totalFailed == 0) {
                    log.info("bulkSuccess^^^bulk done with executionId: {}^^^bulk size:{}^^^no failed docs,do not need to retry", executionId, request.numberOfActions());
                } else {
                    log.info("bulkFailed^^^not need to retry,totalFailed:{}^^^bulk done with executionId: {}^^^bulk size:{}^^^no failed docs,do not need to retry", totalFailed, executionId, request.numberOfActions());
                }
                saveMetric(baseMetrics, request.numberOfActions(), totalFailed, toBeTry);
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                log.info("after bulk2 got exception^^^bulk done with executionId: {}^^^bulk size:{}^^^exception:{}", executionId, request.numberOfActions(), ExceptionUtils.getStackTrace(failure));
                if (failure instanceof NoNodeAvailableException) {
                    log.info("sleep 3*60s after bulk2 NoNodeAvailableException");
                    try {
                        Thread.sleep(3 * 60 * 1000);
                    } catch (InterruptedException e) {
                        log.info("after bulk2 with NoNodeAvailableException^^^error:{}", ExceptionUtils.getStackTrace(e));
                    }
                    if (retryBulkProcessor != null) {
                        for (DocWriteRequest docWriteRequest : request.requests()) {
                            retryBulkProcessor.add(docWriteRequest);
                        }
                    }
                }
                saveMetric(baseMetrics, request.numberOfActions(), request.numberOfActions(), 0);
            }
        };
    }

    private void saveMetric(List<BaseMetric> baseMetrics, int total, int totalFailed, int toBeTry) {
        baseMetrics.forEach(baseMetric -> {
            if (baseMetric instanceof MicroKafkaMetric) {
                MicroKafkaMetric microKafkaMetric = (MicroKafkaMetric) baseMetric;
                //请求总数
                Counter counter = microKafkaMetric.getCounter("elastic.bulk.total1", "elastic.bulk.total", "elastic.bulk.total");
                counter.increment(total);
                //成功数量
                Counter successCounter = microKafkaMetric.getCounter("elastic.bulk.success", "elastic.bulk.success", "elastic.bulk.success");
                successCounter.increment(total - totalFailed);
                //失败数量
                Counter failedCounter = microKafkaMetric.getCounter("elastic.bulk.failed", "elastic.bulk.failed", "elastic.bulk.failed");
                failedCounter.increment(totalFailed);
                //待重试数量{}
                Counter toRetryCounter = microKafkaMetric.getCounter("elastic.bulk.retry", "elastic.bulk.retry", "elastic.bulk.retry");
                toRetryCounter.increment(toBeTry);
                //不重试数量
                Counter noretryCounter = microKafkaMetric.getCounter("elastic.bulk.noretry", "elastic.bulk.noretry", "elastic.bulk.noretry");
                noretryCounter.increment(totalFailed - toBeTry);
            }
        });
    }
}
