package sunrise.demo.table;

import com.alibaba.fastjson2.JSONObject;
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
import sunrise.demo.pojo.Device;
import sunrise.demo.pojo.VideoEvent;

import static org.apache.flink.table.api.Expressions.*;

import org.apache.flink.table.api.*;

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
        properties.setProperty("bootstrap.servers", "datanode:9092");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");
        DataStreamSource<String> kafkaStream = env.addSource(new FlinkKafkaConsumer<String>("dahua_rabbit", new SimpleStringSchema(), properties));
        SingleOutputStreamOperator<Device> videoEventStream = kafkaStream.map(s -> {
            JSONObject device = JSONObject.parse(s);
            Device device1 = new Device();
            device1.setDeviceId((String) device.get("deviceId"));
            return device1;
        }).returns(TypeInformation.of(Device.class));
        Table videoEventTable = tableEnv.fromDataStream(videoEventStream);
        Table video = videoEventTable.select($("deviceId"));
        String sql = "select count(distinct(deviceId)) from video ";
        tableEnv.createTemporaryView("video", video);
        tableEnv.executeSql(sql).print();
        env.execute();

    }
}
