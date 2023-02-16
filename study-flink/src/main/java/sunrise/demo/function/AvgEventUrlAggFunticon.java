package sunrise.demo.function;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import sunrise.demo.pojo.CarInfo;

/**
 * @author kuiqwang
 * @emai wqkenqingto@163.com
 * @time 2023/2/10
 * @desc
 */
public class AvgAggFunticon implements AggregateFunction<CarInfo, Tuple2<Integer, Integer>, Double> {

    @Override
    public Tuple2<Integer, Integer> createAccumulator() {
        return Tuple2.of(0, 0);
    }

    @Override
    public Tuple2<Integer, Integer> add(CarInfo carInfo, Tuple2<Integer, Integer> speedInfo) {
        int speed = carInfo.getCarSpeed();
        //该车辆的车速信息个数
        int number = speedInfo.f1;
        //数量加1
        number += 1;
        //车速总和
        int speedSum = speed += speedInfo.f0;
        return Tuple2.of(speedSum, number);
    }

    @Override
    public Double getResult(Tuple2<Integer, Integer> speedResult) {
        Double res = Double.valueOf(speedResult.f0) / Double.valueOf(speedResult.f1);
        return res;
    }

    @Override
    public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> s1, Tuple2<Integer, Integer> s2) {
        return Tuple2.of(s1.f0 + s2.f0, s1.f1 + s2.f1);
    }
}
