package sunrise.demo.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author kuiqwang
 * @emai wqkenqingto@163.com
 * @time 2023/3/21
 * @desc
 */
public class HylTable {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        String sql = "CREATE TABLE mqtt_cmd (\n" +
                "  `value` INT,\n" +
                "  `uuid` STRING,\n" +
                "  `username` STRING,\n" +
                "  `uid` STRING,\n" +
                "  `topic` STRING,\n" +
                "  `timestamp` BIGINT,\n" +
                "  `priority_2` INT,\n" +
                "  `priority_1` INT,\n" +
                "  `name` STRING,\n" +
                "  `dataType` STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'mqtt_cmd',\n" +
                "  'properties.bootstrap.servers' = 'kafka01:9092,kafka02:9092,kafka03:9092',\n" +
                "  'properties.group.id' = 'cmd01',\n" +
                "  'format' = 'json',\n" +
                "  'json.fail-on-missing-field' = 'false',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'json.ignore-parse-errors' = 'true'\n" +
                ")\n";
        String sql3 = "CREATE TABLE mqtt_resp (\n" +
                "  uuid STRING,\n" +
                "  username STRING,\n" +
                "  uid STRING,\n" +
                "  topic_req STRING,\n" +
                "  error INT\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'mqtt_response',\n" +
                "  'properties.bootstrap.servers' = 'kafka01:9092,kafka02:9092,kafka03:9092',\n" +
                "  'properties.group.id' = 'cmd01',\n" +
                "  'format' = 'json',\n" +
                "  'json.fail-on-missing-field' = 'false',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'json.ignore-parse-errors' = 'true'\n" +
                ")";

        tableEnv.executeSql(sql);
        tableEnv.executeSql(sql3);
        String sql2 = "select * from mqtt_cmd  m    LEFT join  mqtt_resp r  on m.uuid = r.uuid  where topic_req >";
//        String sql2 = "select * from mqtt_resp";
        tableEnv.executeSql(sql2).print();
    }
}
