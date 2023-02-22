package sunrise.demo.source;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.jdbc.JdbcInputFormat;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.math.BigInteger;

/**
 * @author kuiqwang
 * @emai wqkenqingto@163.com
 * @time 2023/2/21
 * @desc
 */
public class MysqlSourceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //flink内置的有 socket、file、collection等
        /** flink mysql  connector*/
        // configure the JDBC input format
        JdbcInputFormat jdbcInputFormat = JdbcInputFormat.buildJdbcInputFormat()
                .setDrivername("com.mysql.jdbc.Driver")
                .setDBUrl("jdbc:mysql://home.kuiq.wang:3307/evn")
                .setUsername("root")
                .setPassword("server123")
                .setQuery("SELECT id, name FROM data_source")
                .setRowTypeInfo(new RowTypeInfo(TypeInformation.of(BigInteger.class), TypeInformation.of(String.class)))
                .finish();

//        // read the data from MySQL into a DataSet
//        env.createInput(jdbcInputFormat)
//                .map((MapFunction<Row, Tuple2<BigInteger,String>>) row -> {
//                    BigInteger res = (BigInteger) row.getField(0);
//                    return Tuple2.of(res, (String) row.getField(1));}).returns(Types.TUPLE(Types.BIG_INT, Types.STRING))
//                .print();

        env.execute();
    }
}
