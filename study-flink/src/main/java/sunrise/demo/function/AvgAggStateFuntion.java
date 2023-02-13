package sunrise.demo.function;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import sunrise.demo.pojo.CarInfo;

/**
 * @author kuiqwang
 * @emai wqkenqingto@163.com
 * @time 2023/2/10
 * @desc
 */
public class AvgAggStateFuntion extends KeyedProcessFunction<String, CarInfo, Tuple2<String, Double>> {


    private transient AggregatingState aggregatingState;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        //定义描述器
        AggregatingStateDescriptor aggregatingStateDescriptor = new AggregatingStateDescriptor(
                "avg-spped",
                new AvgAggFunticon(),
                TypeInformation.of(new TypeHint<Tuple2<Double, Integer>>() {
                }));
        aggregatingState = getRuntimeContext().getAggregatingState(aggregatingStateDescriptor);

    }


    @Override
    public void processElement(CarInfo carInfo, KeyedProcessFunction<String, CarInfo, Tuple2<String, Double>>.Context context, Collector<Tuple2<String, Double>> collector) throws Exception {
        aggregatingState.add(carInfo);
        collector.collect(Tuple2.of(carInfo.getCarNumber(), (Double) aggregatingState.get()));
    }
}
