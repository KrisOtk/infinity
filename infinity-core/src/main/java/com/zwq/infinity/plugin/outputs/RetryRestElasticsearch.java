package com.zwq.infinity.plugin.outputs;

import com.zwq.infinity.baseplugin.BaseMetric;
import com.zwq.infinity.plugin.metrics.MicroKafkaMetric;
import com.zwq.infinity.util.InfinityJsonUtil;
import io.micrometer.core.instrument.Counter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.sniff.Sniffer;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


/**
 * created by kris on 12/10/18.
 */
@Slf4j
public class RetryRestElasticsearch {
    private BulkProcessor retryBulkProcessor;
    private final static int BULKACTION = 20000;
    private final static int BULKSIZE = 15; //MB
    private final static int FLUSHINTERVAL = 60;
    private final static int CONCURRENTREQSIZE = 1;
    List<BaseMetric> baseMetrics ;

    public BulkProcessor getRetryBulkProcessor() {
        return retryBulkProcessor;
    }

    public RetryRestElasticsearch(List<BaseMetric> baseMetrics) {
        this.baseMetrics = baseMetrics;
    }

    public void setRetryBulkProcessor(BulkProcessor retryBulkProcessor) {
        this.retryBulkProcessor = retryBulkProcessor;
    }

     void buildRetryBulkProcessor(Map<String, Object> config) {
        if (config == null || config.isEmpty()) {
            log.info("RetryBulkProcessor构造失败,因为缺少相关配置");
            return;
        }

        List<String> hosts = (ArrayList<String>) config.get("hosts");
        int bulkActions = config.containsKey("bulk_actions") ? (int) config.get("bulk_actions") : BULKACTION;
        int bulkSize = config.containsKey("bulk_size") ? (int) config.get("bulk_size") : BULKSIZE;

         String username = config.containsKey("username") ? (String) config.get("username") : "";
         String password = config.containsKey("password") ? (String) config.get("password") : "";


         final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
         credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));


        //初始化 RestHighLevelClient
         RestHighLevelClient restClient = new RestHighLevelClient(
                 RestClient.builder(hosts.stream()
                         .map(host -> host.split(":")).filter(a -> a.length == 2)
                         .map(ipAndPort -> new HttpHost(ipAndPort[0], Integer.parseInt(ipAndPort[1]), "http"))
                         .toArray(HttpHost[]::new))
                         .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider))
         );

         boolean sniff = !(config.containsKey("sniff") && !((boolean) config.get("sniff")));
        log.info("重试restClient^^^是否开启节点嗅探机制:{}", sniff);
        if (sniff) {
            Sniffer.builder(restClient.getLowLevelClient()).build();
        }
        //通过配置获取bulk api的回调
        BulkProcessor.Listener bulkLlstener = getBulkListener(baseMetrics);
         this.retryBulkProcessor = BulkProcessor.builder((bulkRequest, listener) -> restClient.bulkAsync(bulkRequest, RequestOptions.DEFAULT, listener), bulkLlstener)
                .setBulkActions(bulkActions)
                .setBulkSize(new ByteSizeValue(bulkSize, ByteSizeUnit.MB))
                 .setFlushInterval(TimeValue.timeValueSeconds(FLUSHINTERVAL))
                 .setConcurrentRequests(CONCURRENTREQSIZE)//默认是1，表示积累bulk requests和发送bulk是异步的，其数值表示发送bulk的并发线程数，设置为0表示二者同步的
                .build();
    }

    private  BulkProcessor.Listener getBulkListener(List<BaseMetric> baseMetrics) {
        return new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                log.info("等待重试retry^^^executionId:{}^^^numberOfActions:{}", executionId, request.numberOfActions());
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                List<DocWriteRequest<?>> requests = request.requests();
                int noRetry = 0;
                int totalFailed = 0;
                for (BulkItemResponse item : response.getItems()) {
                    if (item.isFailed()) {
                        switch (item.getFailure().getStatus()) {
                            //前面两种默认去重试 第一个没有break,所以会执行到第一个break的地方
                            case TOO_MANY_REQUESTS:
                            case SERVICE_UNAVAILABLE:
                                noRetry++;
                                //重试失败 不再进行处理,只打印失败的请求
                                log.info("retry^^^bulk has failed item which can not to retry.message:{},item:{}", item.getFailureMessage(), InfinityJsonUtil.toJSON(requests.get(item.getItemId())));
                                break;
                            default:
                                //默认不处理了
                                break;
                        }
                        totalFailed++;
                    }
                }
                if (noRetry > 0) {
                    log.info("retry^^^bulkFailed^^^bulk done with executionId: {}^^^bulk size:{}^^^{} doc failed,{} need to retry", executionId, request.numberOfActions(), totalFailed, noRetry);
                    try {
                        Thread.sleep(noRetry / 2);
                        log.info("retry^^^sleep " + noRetry / 2 + "milliseconds after bulk failure");
                    } catch (InterruptedException e) {
                        log.info("retry^^^after bulk^^^thread sleep failed error,InterruptedException:{}", ExceptionUtils.getStackTrace(e));
                    }
                } else {
                    log.info("retry^^^bulkSuccess^^^bulk done with executionId: {}^^^bulk size:{}^^^no failed docs,do not need to retry", executionId, request.numberOfActions());
                }
                saveMetric(baseMetrics, request.numberOfActions(), totalFailed, noRetry);
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                log.info("retry^^^bulk done with executionId: {}^^^bulk size:{}^^^after bulk2 got exception:{}", executionId, request.numberOfActions(), ExceptionUtils.getStackTrace(failure));
                if (failure instanceof NoNodeAvailableException) {
                    log.info("retry^^^sleep 3*60s after bulk2 NoNodeAvailableException");
                    try {
                        Thread.sleep(3 * 60 * 1000);
                    } catch (InterruptedException e) {
                        log.info("retry^^^after bulk2 with NoNodeAvailableException^^^error:{}", ExceptionUtils.getStackTrace(e));
                    }
                    for (DocWriteRequest docWriteRequest : request.requests()) {
                        log.info("retry^^^bulk has failed item which can not to retry.item:{}", InfinityJsonUtil.toJSON(docWriteRequest));
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
                Counter counter = microKafkaMetric.getCounter("elastic.bulk.retry.total", "elastic.bulk.retry.total", "elastic.bulk.retry.total");
                counter.increment(total);
                //成功数量
                Counter successCounter = microKafkaMetric.getCounter("elastic.bulk.retry.success", "elastic.bulk.retry.success", "elastic.bulk.retry.success");
                successCounter.increment(total - totalFailed);
                //失败数量
                Counter failedCounter = microKafkaMetric.getCounter("elastic.bulk.retry.failed", "elastic.bulk.retry.failed", "elastic.bulk.retry.failed");
                failedCounter.increment(totalFailed);
                //待重试数量
                Counter toRetryCounter = microKafkaMetric.getCounter("elastic.bulk.retry.noretry", "elastic.bulk.retry.noretry", "elastic.bulk.retry.noretry");
                toRetryCounter.increment(toBeTry);
            }
        });
    }

}


