package sunrise.demo.source;/**
 * @author kuiqwang
 * @emai wqkenqingto@163.com
 * @time 2022/11/15
 * @desc
 */

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.*;

public class ClickSource extends RichSourceFunction<Tuple2<String, String>> {


    private final String query;
    private transient Connection connection;

    public ClickSource(String query) {
        this.query = query;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = DriverManager.getConnection("jdbc:clickhouse://ck01:8123/dbp", "yanggu", "yg123456");
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (connection != null) {
            connection.close();
        }
    }

    @Override
    public void run(SourceContext<Tuple2<String, String>> sourceContext) throws Exception {
        try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet = statement.executeQuery(query)) {
                while (resultSet.next()) {

                    sourceContext.collect(Tuple2.of(resultSet.getString(1), resultSet.getString(2)));
                }
            }
        }
    }

    @Override
    public void cancel() {
        // 可以为空，因为在 run 方法中通过 while 循环判断是否被取消
    }
}
