package sunrise.demo.table;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import sunrise.demo.pojo.NcpDetail;
import sunrise.demo.pojo.TUser;
import sunrise.demo.source.MysqlReaderNew;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author kuiqwang
 * @emai wqkenqingto@163.com
 * @time 2023/2/27
 * @desc 重点使用join算子
 */
public class TableDemo5 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String username = "root";
        String password = "Yg123456";
        String dbUrl = "jdbc:mysql://namenode/fkb";
        String sql = "select * from ncp_detail";
        String sql2 = "select * from t_user";
        String demosql = "select u.name, u.phone ,n.* from ncp_detail n join t_user u on u.id=n.user_id";
        DataStreamSource<Object> ncpStream = env.addSource(new MysqlReaderNew(dbUrl, username, password, sql, NcpDetail.class));
        DataStreamSource<Object> userStream = env.addSource(new MysqlReaderNew(dbUrl, username, password, sql2, TUser.class));
        SingleOutputStreamOperator<NcpDetail> nsingle = ncpStream.map(s -> {
            ObjectMapper mapper = new ObjectMapper();
            return mapper.convertValue(s, NcpDetail.class);
        });
        SingleOutputStreamOperator<TUser> usingle = userStream.map(s -> {
            ObjectMapper mapper = new ObjectMapper();
            return mapper.convertValue(s, TUser.class);
        });

        Table ncp = tableEnv.fromDataStream(nsingle).as("n").renameColumns($("id").as("nid")).select($("nid"), $("health"));
        Table user = tableEnv.fromDataStream(usingle).as("u").select($("id"), $("name"));
//        ncp.join(user).where($("userId").isEqual($("id")))
//                .execute().print();
        ncp.unionAll(user).execute().print();

    }
}
