package sunrise.demo.stream.practice;

import com.alibaba.fastjson2.JSONObject;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.http.HttpHost;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;
import sunrise.demo.pojo.VideoEvent;
import sunrise.demo.stream.mapper.RedisSinkMapper;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @author kuiqwang
 * @emai wqkenqingto@163.com
 * @time 2023/2/20
 * @desc
 */
public class JllCarAvgSpeedCount {
    public static void main(String[] args) throws Exception {
        ParameterTool parameter = ParameterTool.fromArgs(args);
        String sourceTopic = parameter.get("sourceTopic");
        String sinkTopic= parameter.get("sinkTopic");
        //source is kafka , sink is elasticsearch。
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "kafka01:9092,kafka02:9092,kafka03:9092");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "earliest");
        DataStreamSource<String> videoEventStream = env.addSource(new FlinkKafkaConsumer<String>(sourceTopic, new SimpleStringSchema(), properties));
        SingleOutputStreamOperator<VideoEvent> singleStream = videoEventStream.map(s ->
        {
            ObjectMapper objectMapper = new ObjectMapper();
            return objectMapper.readValue(s, VideoEvent.class);
        }).returns(TypeInformation.of(VideoEvent.class)).assignTimestampsAndWatermarks(WatermarkStrategy.<VideoEvent>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<VideoEvent>() {
            @Override
            public long extractTimestamp(VideoEvent videoEvent, long l) {
                return LocalDateTime.parse(videoEvent.getTime(), DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS")).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
            }
        }));
        /** elasticsearch sink*/
//        List<HttpHost> hosts = new ArrayList<>();
//        String host = "192.168.10.241";
//        hosts.add(new HttpHost(host, 9200, "http"));
//        // 添加esSink
//        ElasticsearchSinkFunction<VideoEvent> sinkFunction = (VideoEvent event, RuntimeContext runtimeContext, RequestIndexer requestIndexer) -> {
//            String vjson = JSONObject.toJSONString(event);
//            requestIndexer.add(Requests.indexRequest().index("jll_demo").source(vjson, XContentType.JSON));
//        };
//        ElasticsearchSink.Builder<VideoEvent> esSinkBuilder = new ElasticsearchSink.Builder<VideoEvent>(hosts, sinkFunction);
//        singleStream.addSink(esSinkBuilder.build());
//        /**redis sink */
//        FlinkJedisPoolConfig poolConfig = new FlinkJedisPoolConfig.Builder().setHost("192.168.3.9").setPassword("home123").setDatabase(1).build();
//        singleStream.addSink(new RedisSink<>(poolConfig, new RedisSinkMapper()));

//        /** kafka sink*/
        String kafkaBroker = "es01:9092";
        String topicName = "jll-test";
        Properties producerProps = new Properties();
        producerProps.setProperty("bootstrap.servers", kafkaBroker);
        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>(
                sinkTopic,
                new SimpleStringSchema(),
                producerProps
        );
        videoEventStream.addSink(kafkaProducer);
        env.execute();
    }
}
