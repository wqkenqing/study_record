package sunrise.demo.function;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
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
public class CarReduceFuntion extends KeyedProcessFunction<String, CarInfo, CarInfo> {

    private transient ReducingState carReduceSpeed;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ReduceFunction<CarInfo> carFunction = (CarInfo car1, CarInfo car2) -> {
            int newSpeed = car1.getCarSpeed() + car2.getCarSpeed();
            car1.setCarSpeed(newSpeed);
            return car1;
        };
        ReducingStateDescriptor<CarInfo> carSpeedDesc = new ReducingStateDescriptor<CarInfo>(
                "last-speed", carFunction,
                CarInfo.class);
        carReduceSpeed = getRuntimeContext().getReducingState(carSpeedDesc);
    }


    @Override
    public void processElement(CarInfo carInfo, KeyedProcessFunction<String, CarInfo, CarInfo>.Context context, Collector<CarInfo> collector) throws Exception {
        carReduceSpeed.add(carInfo);
        collector.collect((CarInfo) carReduceSpeed.get());

    }
}
