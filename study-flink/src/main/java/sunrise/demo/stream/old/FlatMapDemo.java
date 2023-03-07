package sunrise.demo.stream.old;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author kuiqwang
 * @emai wqkenqingto@163.com
 * @time 2023/2/10
 * @desc
 */
public class FlatMapDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> words = env.socketTextStream("localhost", 8882);
        FlatMapFunction wordFlatMap = new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                for (String ss : s.split("\\s+")) {
                    collector.collect(Tuple2.of(ss, 1));
                }
                ;
            }
        };
//        words.flatMap(wordFlatMap).print();
        words.flatMap((String s, Collector<Tuple2<String, Integer>> collector) -> {
                    for (String ss : s.split("\\s+")) {
                        collector.collect(Tuple2.of(ss, 1));
                    }
                }).returns(Types.TUPLE(Types.STRING, Types.INT))
                .print();
        env.execute();

    }
}
