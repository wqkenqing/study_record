package sunrise.demo.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author kuiqwang
 * @emai wqkenqingto@163.com
 * @time 2023/3/2
 * @desc
 */
public class TableDemo6 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        String sql = "CREATE TABLE att_business (\n" +
                "  id String,\n" +
                "  project_id String,\n" +
                "  project_code String" +
                ") WITH (\n" +
                " 'connector' = 'filesystem',\n" +
                " 'path' = '/Users/kuiqwang/Desktop/att_business.csv',\n" +
                " 'format' = 'csv'" +
                ")";
        String kafkaSql = "CREATE TABLE KafkaTable (\n" +
                "  `deviceStatus` String,\n" +
                "  `describe` String,\n" +
                "  `postionNo` STRING,\n" +
                "  `creatTime` TIMESTAMP(3) METADATA FROM 'timestamp'\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'jllsd-flume-collect-from-yaobo-1',\n" +
                "  'properties.bootstrap.servers' = 'kafka01:9092',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'json.ignore-parse-errors' = 'true',\n" +
                "  'format' = 'json'\n" +
                ")";
        String esSql = "CREATE TABLE myUserTable (\n" +
                "id BiGINT,"+
                "  name String,\n" +
                "  filePath STRING,\n" +
                "  fileSize BIGINT,\n" +
                "  PRIMARY KEY (id) NOT ENFORCED\n"+
                ") WITH (\n" +
                "  'connector' = 'elasticsearch-7',\n" +
                "  'hosts' = 'http://calculation02:9200',\n" +
                "  'index' = 'sxsddsj-file-system'\n" +
                ")";
        String hbaseSql = "CREATE TABLE hTable (\n" +
                " rowkey String,\n" +
                "  info ROW<name String,remark String>,\n" +
                " PRIMARY KEY (rowkey) NOT ENFORCED\n" +
                ") WITH (\n" +
                " 'connector' = 'hbase-2.2',\n" +
                " 'table-name' = 'cs_user1',\n" +
                " 'zookeeper.quorum' = 'kafka01:2181'\n" +
                ")";
        System.out.println(hbaseSql);
        tableEnv.executeSql(kafkaSql);
        tableEnv.sqlQuery("select * from KafkaTable").execute().print();

    }
}
