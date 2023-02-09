package sunrise.demo.stream;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;

/**
 * @author kuiqwang
 * @emai wqkenqingto@163.com
 * @time 2023/2/8
 * @desc
 */
public class SplitDemo {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataSource = env.socketTextStream("localhost", 8882);

        dataSource.getSideOutput()
    }
}
