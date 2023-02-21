package sunrise.demo.function.agg;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import sunrise.demo.pojo.CarInfo;
import sunrise.demo.pojo.VideoEvent;

/**
 * @author kuiqwang
 * @emai wqkenqingto@163.com
 * @time 2023/2/10
 * @desc
 */
public class VideoSpeedAvgAggFunticon implements AggregateFunction<VideoEvent, Tuple2<Integer, Integer>, Double> {


    @Override
    public Tuple2<Integer, Integer> createAccumulator() {
        return Tuple2.of(0, 0);
    }

    @Override
    public Tuple2<Integer, Integer> add(VideoEvent videoEvent, Tuple2<Integer, Integer> integerIntegerTuple2) {

        return Tuple2.of(integerIntegerTuple2.f0 + videoEvent.getSpeed(), integerIntegerTuple2.f1 + 1);
    }

    @Override
    public Double getResult(Tuple2<Integer, Integer> integerIntegerTuple2) {

        return Double.valueOf(integerIntegerTuple2.f0) / Double.valueOf(integerIntegerTuple2.f1);
    }

    @Override
    public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> integerIntegerTuple2, Tuple2<Integer, Integer> acc1) {
        return Tuple2.of(integerIntegerTuple2.f0 + acc1.f0, integerIntegerTuple2.f1 + acc1.f1);
    }
}
