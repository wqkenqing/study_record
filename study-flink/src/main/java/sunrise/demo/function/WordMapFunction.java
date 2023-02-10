package sunrise.demo.function;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import sunrise.demo.pojo.CarInfo;

import java.util.Map;

/**
 * @author kuiqwang
 * @emai wqkenqingto@163.com
 * @time 2023/2/9
 * @desc
 */

public class WordMapFunction extends RichMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>> {
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
    public Tuple2<String, Integer> map(Tuple2<String, Integer> s) throws Exception {
        if (null == mapState.get(s.f0)) {
            mapState.put(s.f0, 1);
        } else {
            mapState.put(s.f0, mapState.get(s.f0) + 1);
        }
        return Tuple2.of(s.f0, mapState.get(s.f0));
    }
}
