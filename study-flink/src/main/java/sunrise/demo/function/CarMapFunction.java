package sunrise.demo.function;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import sunrise.demo.pojo.CarInfo;

import java.util.Map;

/**
 * @author kuiqwang
 * @emai wqkenqingto@163.com
 * @time 2023/2/9
 * @desc
 */

public class CarMapFunction extends RichMapFunction<CarInfo, Map<String,Integer>> {

    //车辆最后的车速
    private transient MapState<String, Integer> carMapCount;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        MapStateDescriptor<String, Integer> lastCarNumberCount = new MapStateDescriptor<String, Integer>(
                "last-speed",
                String.class,Integer.class);
        carMapCount = getRuntimeContext().getMapState(lastCarNumberCount);
    }




    @Override
    public Map<String,Integer> map(CarInfo carInfo) throws Exception {
        if (carMapCount.get(carInfo.getCarNumber()) == null) {
            carMapCount.put(carInfo.getCarNumber(), 1);
        }

    }
}
