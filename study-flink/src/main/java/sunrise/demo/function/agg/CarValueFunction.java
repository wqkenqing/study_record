package sunrise.demo.function.agg;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import sunrise.demo.pojo.CarInfo;

/**
 * @author kuiqwang
 * @emai wqkenqingto@163.com
 * @time 2023/2/9
 * @desc
 */

public class CarValueFunction extends KeyedProcessFunction<String, CarInfo, CarInfo> {

    //车辆最后的车速
    private transient ValueState<Integer> lastCarSpeed;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        ValueStateDescriptor<Integer> lastSpeedDesc = new ValueStateDescriptor<Integer>(
                "last-speed",
                Integer.class);
        lastCarSpeed = getRuntimeContext().getState(lastSpeedDesc);

    }

    @Override
    public void processElement(CarInfo carInfo, KeyedProcessFunction<String, CarInfo, CarInfo>.Context context, Collector<CarInfo> collector) throws Exception {
        if (lastCarSpeed.value() == null) {
            lastCarSpeed.update(carInfo.getCarSpeed());
        } else if (carInfo.getCarSpeed() - lastCarSpeed.value() > 0) {
            lastCarSpeed.update(carInfo.getCarSpeed());
            collector.collect(carInfo);
        }
    }
}
