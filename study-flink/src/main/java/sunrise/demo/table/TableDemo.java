package sunrise.demo.table;

import com.esotericsoftware.kryo.util.ObjectMap;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import sunrise.demo.pojo.VideoEvent;

import java.util.Properties;

/**
 * @author kuiqwang
 * @emai wqkenqingto@163.com
 * @time 2023/2/23
 * @desc flink table and sql 的使用
 */
public class TableDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //获取table env
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "kafka01:9092,kafka02:9092,kafka03:9092");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "earliest");
        DataStreamSource<String> kafkaStream = env.addSource(new FlinkKafkaConsumer<String>("jllsd-video-analysis-data", new SimpleStringSchema(), properties));
        SingleOutputStreamOperator<VideoEvent> videoEventStream = kafkaStream.map(s -> {
            ObjectMapper mapper = new ObjectMapper();
            return mapper.readValue(s, VideoEvent.class);
        }).returns(TypeInformation.of(VideoEvent.class));
        Table videoEventTable = tableEnv.fromDataStream(videoEventStream);
        Table res = tableEnv.sqlQuery("select * from " + videoEventTable);
        tableEnv.toDataStream(res).print();
        env.execute();

    }
}
