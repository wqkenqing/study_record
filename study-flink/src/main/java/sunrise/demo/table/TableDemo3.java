package sunrise.demo.table;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import sunrise.demo.pojo.GoodsRecord;
import sunrise.demo.source.MysqlReader;
import sunrise.demo.util.ORMUtil;

import java.util.stream.Collectors;

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
        String sql = "select * from goods_record";
//        JdbcInputFormat  goodsInput = JdbcInputFormat.buildJdbcInputFormat()
//                .setDrivername("com.mysql.jdbc.Driver")
//                .setDBUrl(dbUrl)
//                .setUsername(username)
//                .setPassword(password)
//                .setQuery(sql)
//                .setRowTypeInfo(new RowTypeInfo())
//                .finish();
        DataStreamSource<GoodsRecord> goodsRecord = env.addSource(new MysqlReader(dbUrl, username, password, sql));
        Table goods = tableEnv.fromDataStream(goodsRecord).filter($("user").isNotNull());
        Table goodss = goods.where($("user").isEqual("魏新"));
        goodss.execute().print();
    }
}
