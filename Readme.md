### 启动demo
--- 
> 代码中默认是使用的时区是上海-Asia/Shanghai

1. 方式1-使用环境变量进行启动

```shell
# 设置环境变量(需要对配置进行转义)
export xx="{\"inputs\":[{\"Kafka\":{\"topic\":{\"testTopic\":2},\"consumer_settings\":{\"bootstrap.servers\":\"10.9.35.157:9092\",\"key.deserializer\":\"org.apache.kafka.common.serialization.StringDeserializer\",\"value.deserializer\":\"org.apache.kafka.common.serialization.StringDeserializer\",\"group.id\":\"testTopicConsumer\",\"enable.auto.commit\":true},\"codec\":\"json\"}}],\"filters\":[{\"Grok\":{\"src\":\"message\",\"lengthLimit\":10000,\"match\":[\"%{GREEDYDATA:logcontent}\"],\"encoding\":"UTF8",\"if\":[],\"tag_on_failure\":null,\"remove_fields\":[\"message\"]}}],\"outputs\":[{\"RestElasticsearch\":{\"cluster\":[\"test_cluster\"],\"hosts\":[\"10.9.36.22:9200\"],\"index\":\"test-topic-%{+YYYY.MM.dd}\",\"index_type\":\"logs\",\"document_id\":null,\"route\":null,\"timezone\":\"Asia/Shanghai\",\"bulk_actions\":10000,\"bulk_size\":10,\"flush_interval\":15,\"sniff\":true}}]}"
# 使用环境变量启动   
java -DuseConfigMap=true -DconfigMapName=xx -jar infinity-core-0.0.1-SNAPSHOT.jar 
```

2.方式2-使用配置文件进行启动

```shell
# 启动参数
-Dconfig.file=./infinity-core/src/main/resources/config/test/1-stdin_to_stdout.yml
-Dconfig.file=/path/to/config/put2kafka.yml
```

