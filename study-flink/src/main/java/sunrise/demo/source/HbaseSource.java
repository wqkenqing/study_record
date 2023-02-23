package sunrise.demo.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author kuiqwang
 * @emai wqkenqingto@163.com
 * @time 2023/2/22
 * @desc
 */
public class HbaseSource implements SourceFunction<String> {
    private String tableName;
    private transient Connection connection;

    public HbaseSource(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "kafka01:2181");
        connection = ConnectionFactory.createConnection(config);
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            String value = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("name")));
            sourceContext.collect(value);
        }
    }

    @Override
    public void cancel() {
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (Exception e) {
            // do nothing
        }
    }
}
