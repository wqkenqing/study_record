package sunrise.demo.state;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import sunrise.demo.function.agg.WordFlatMapFunction;

/**
 * @author kuiqwang
 * @emai wqkenqingto@163.com
 * @time 2023/2/9
 * @desc
 */
public class MapStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> worCountStream = env.socketTextStream("localhost", 8882);
        KeySelector<Tuple2<String, Integer>, String> wordCount = new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> words) throws Exception {
                return words.f0;
            }
        };
//        worCountStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
//                                   @Override
//                                   public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
//                                       for (String ss : s.split("\\s+")) {
//                                           collector.collect(Tuple2.of(ss, 1));
//                                       }
//                                   }
//                               }
//                ).keyBy(0).flatMap(new WordFlatMapFunction())
//                .print();
        worCountStream.flatMap((String s, Collector<Tuple2<String,Integer>>out) -> {
                    for (String ss : s.split("\\s+")) {
                        out.collect(Tuple2.of(ss, 1));
                    }
                }).returns( TypeInformation.of(new TypeHint<Tuple2<String,Integer>>() {})).keyBy(0).flatMap(new WordFlatMapFunction())
                .print();
        env.execute();
    }
}
