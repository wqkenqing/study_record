package sunrise.demo.sql;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import sunrise.demo.pojo.NcpDetail;
import sunrise.demo.source.MysqlReaderNew;
import sunrise.demo.util.DBinfo;

/**
 * @author kuiqwang
 * @emai wqkenqingto@163.com
 * @time 2023/2/27
 * @desc flink sql  query的应用
 */
public class SqlDemo1 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        String sql = "select * from ncp_detail";
        String sql2 = "select userId,health from ncp_detail";

        /**这里是要将flink sql 用起来*/
        //1. 创建table
        DataStreamSource<Object> ncpDetailStream = env.addSource(new MysqlReaderNew(DBinfo.DBURL, DBinfo.USERNAME, DBinfo.PASSWORD, sql, NcpDetail.class));
        SingleOutputStreamOperator<NcpDetail> nstream = ncpDetailStream.map(s -> {
            ObjectMapper objectMapper = new ObjectMapper();
            return objectMapper.convertValue(s, NcpDetail.class);
        });
        Table ntable = tableEnv.fromDataStream(nstream);
        tableEnv.createTemporaryView("ntable", ntable);
        String sql3 = "CREATE TABLE ntemp (\n" +
                "  userId BIGINT,\n" +
                "  health  STRING\n"+
                ") WITH (\n" +
                "  'connector' = 'filesystem',\n" +
                "  'path' = '/Users/kuiqwang/Desktop/flnk_sql/ntemp.csv',\n" +
                "  'format' = 'csv'\n" +
                ")\n";
        // create and register a TableSink
        tableEnv.executeSql(sql3);
        tableEnv.executeSql("insert into ntemp select userId,health from ntable").print();
    }
}
