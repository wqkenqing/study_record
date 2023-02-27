package sunrise.demo.table;


import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import sunrise.demo.pojo.NcpDetail;
import sunrise.demo.source.MysqlReaderNew;


import java.sql.Timestamp;

import static org.apache.flink.table.api.Expressions.*;

/**
 * @author kuiqwang
 * @emai wqkenqingto@163.com
 * @time 2023/2/26
 * @desc table api 熟悉,本类中重点熟悉了table and window中的相关使用
 */
public class TableDemo4 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        String username = "root";
        String password = "Yg123456";
        String dbUrl = "jdbc:mysql://namenode/fkb";
        String sql = "select * from ncp_detail";
        DataStreamSource<Object> goods = env.addSource(new MysqlReaderNew(dbUrl, username, password, sql, NcpDetail.class));
        SingleOutputStreamOperator<NcpDetail> goodRecord = goods.map(s -> {
            ObjectMapper mapper = new ObjectMapper();
            return mapper.convertValue(s, NcpDetail.class);
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<NcpDetail>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<NcpDetail>() {
            @Override
            public long extractTimestamp(NcpDetail ncpDetail, long l) {
                return ncpDetail.getReportDate().getTime();
            }
        }));
        /**
         *
         * .rowtime()这个方法的指定使用，这几天所面临问题中解决问题的核心一笔。
         *
         * */
        Table gtable = tableEnv.fromDataStream(goodRecord, $("userId"), $("reportDate").rowtime());
        gtable.printSchema();
        gtable
                .window(Tumble.over(lit(5).minutes()).on($("reportDate")).as("w"))
                .groupBy($("w"))
                .select($("w").start(), $("userId").count()).execute().print();

    }

    /**
     * cal 函数的使用
     */
    public static class MyToTimestampFunction extends ScalarFunction {
        public Timestamp eval(Timestamp strDate) {
            try {
                return new Timestamp(strDate.getTime());
            } catch (Exception e) {

                throw new RuntimeException("Error parsing date: " + strDate, e);
            }
        }
    }
}
