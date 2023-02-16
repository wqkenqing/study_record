package sunrise.demo.function;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import sunrise.demo.pojo.Event;

import java.nio.charset.StandardCharsets;

/**
 * @author kuiqwang
 * @emai wqkenqingto@163.com
 * @time 2023/2/10
 * @desc
 */
public class AvgEventUrlAggFunticon implements AggregateFunction<Event, Tuple2<Integer, Integer>, Double> {


    @Override
    public Tuple2<Integer, Integer> createAccumulator() {
        return Tuple2.of(0, 0);
    }

    @Override
    public Tuple2<Integer, Integer> add(Event event, Tuple2<Integer, Integer> integerIntegerTuple2) {
        Integer urlLength = event.getUrl().length();
        int number = integerIntegerTuple2.f1 + 1;
        int lengthSum = urlLength + integerIntegerTuple2.f0;
        return Tuple2.of(lengthSum, number);
    }

    @Override
    public Double getResult(Tuple2<Integer, Integer> integerIntegerTuple2) {
        Double res = Double.valueOf(integerIntegerTuple2.f0) / Double.valueOf(integerIntegerTuple2.f1);
        return res;
    }

    @Override
    public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> integerIntegerTuple2, Tuple2<Integer, Integer> acc1) {

        return Tuple2.of(integerIntegerTuple2.f0 + integerIntegerTuple2.f0, integerIntegerTuple2.f1 + integerIntegerTuple2.f1);
    }
}
