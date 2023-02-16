package sunrise.demo.stream.old;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author kuiqwang
 * @emai wqkenqingto@163.com
 * @time 2023/2/8
 * @desc
 */
public class KeyDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource socketSource = env.socketTextStream("0.0.0.0", 8882);
        KeySelector<Tuple2<String, Integer>, String> keySelector = new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> keys) throws Exception {
                return keys.f0;
            }
        };
        ReduceFunction<Tuple2<String, Integer>> keyReduce = (Tuple2<String, Integer> s1, Tuple2<String, Integer> s2) -> {
            return Tuple2.of(s1.f0, s1.f1 + s2.f1);
        };
        socketSource.map(s -> Tuple2.of(s, 1)).returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(keySelector)
                .reduce(keyReduce).returns(Types.TUPLE(Types.STRING, Types.INT))
                .print();
        env.execute();
    }
}
