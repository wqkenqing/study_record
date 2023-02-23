package sunrise.demo.source;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author kuiqwang
 * @emai wqkenqingto@163.com
 * @time 2023/2/22
 * @desc
 */
public class HiveSourceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String sql = "select * from daily_report ";
        DataStreamSource<Tuple2<String,String>> dbpSource = env.addSource(new HiveSource("jdbc:hive2://calculation01:10000/fkb", "", "", sql));
        dbpSource.print();
        env.execute();

    }
}
