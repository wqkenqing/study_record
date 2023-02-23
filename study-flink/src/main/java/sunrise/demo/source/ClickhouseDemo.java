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
public class ClickhouseDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String query = "select * from dbp.dbp_task_monitor";
        DataStreamSource<Tuple2<String, String>> dStream = env.addSource(new ClickSource(query));
        dStream.print();
        env.execute();
    }
}
