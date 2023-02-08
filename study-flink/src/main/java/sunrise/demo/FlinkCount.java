package sunrise.demo;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author kuiqwang
 * @emai wqkenqingto@163.com
 * @time 2023/2/6
 * @desc
 */
public class FlinkCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> textSource = env.readTextFile("/Users/kuiqwang/Desktop/tengxun_class/study-flink/src/main/resources/carFlow_all_column_test.txt");
        textSource.map(s -> Tuple2.of("record", 1)).returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(0).
                countWindowAll(1000)
                .sum(1).print();
        env.execute();
    }

}