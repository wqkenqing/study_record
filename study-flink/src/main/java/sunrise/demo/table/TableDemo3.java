package sunrise.demo.table;

import com.alibaba.fastjson2.JSONObject;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import sunrise.demo.pojo.GoodsRecord;
import sunrise.demo.pojo.NcpDetail;
import sunrise.demo.source.MysqlReaderNew;

import java.nio.charset.StandardCharsets;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author kuiqwang
 * @emai wqkenqingto@163.com
 * @time 2023/2/24
 * @desc
 */
public class TableDemo3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        String username = "root";
        String password = "Yg123456";
        String dbUrl = "jdbc:mysql://namenode/fkb";
        String device = "t_device";
        String device_department = "t_device_department";
//        String sql = "select * from goods_record";
        String sql = "select * from ncp_detail";
//        JdbcInputFormat  goodsInput = JdbcInputFormat.buildJdbcInputFormat()
//                .setDrivername("com.mysql.jdbc.Driver")
//                .setDBUrl(dbUrl)
//                .setUsername(username)
//                .setPassword(password)
//                .setQuery(sql)
//                .setRowTypeInfo(new RowTypeInfo())
//                .finish();
        DataStreamSource<Object> goodsRecord = env.addSource(new MysqlReaderNew(dbUrl, username, password, sql, NcpDetail.class));
        SingleOutputStreamOperator<NcpDetail>sgood= goodsRecord.map(s -> {
            ObjectMapper mapper = new ObjectMapper();
            return mapper.convertValue(s, NcpDetail.class);
        });

//        Table goods = tableEnv.fromDataStream(sgood).filter($("user").isNotNull());
        Table goods = tableEnv.fromDataStream(sgood);
//        Table goodss = goods.where($("user").isEqual("魏新"));
        goods.execute().print();
    }
}
