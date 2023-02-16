package sunrise.demo.function.agg;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

/**
 * @author kuiqwang
 * @emai wqkenqingto@163.com
 * @time 2023/2/9
 * @desc
 */

public class WordFlatMapFunction extends RichFlatMapFunction<Tuple2<String,Integer>, Tuple2<String, Integer>> {
    MapState<String, Integer> mapState = null;

    //车辆最后的车速
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        MapStateDescriptor<String, Integer> wordCountMap = new MapStateDescriptor<String, Integer>(
                "word-count",
                String.class, Integer.class);
        mapState = getRuntimeContext().getMapState(wordCountMap);

    }


    @Override
    public void flatMap(Tuple2<String,Integer> word, Collector<Tuple2<String, Integer>> collector) throws Exception {
        String[] words = word.f0.split("\\s+");
        for (String w : words) {
            if (mapState.get(w) == null) {
                mapState.put(w, 1);
                collector.collect(Tuple2.of(w, 1));
            } else {
                int newval = mapState.get(w) + 1;
                mapState.put(w, newval);
                collector.collect(Tuple2.of(w, newval));
            }
        }

    }
}
