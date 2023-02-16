package sunrise.demo.function.process;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import sunrise.demo.pojo.CarInfo;

/**
 * @author kuiqwang
 * @emai wqkenqingto@163.com
 * @time 2023/2/16
 * @desc
 */
@Slf4j
public class ProcessFunctionDemo extends ProcessFunction<CarInfo,CarInfo> {
    @Override
    public void processElement(CarInfo carinfo, ProcessFunction<CarInfo, CarInfo>.Context context, Collector<CarInfo> collector) throws Exception {
        String carNumber = carinfo.getCarNumber();
        Integer number = Integer.valueOf(carNumber.replace("carNo-", ""));
        Integer speed = Integer.valueOf(carinfo.getCarSpeed());
        if (number > 55 && speed > 80) {
            //限定了车牌和车速
            collector.collect(carinfo);
        }
    }
}
