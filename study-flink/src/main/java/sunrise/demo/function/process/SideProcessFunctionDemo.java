package sunrise.demo.function.process;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import sunrise.demo.pojo.CarInfo;

/**
 * @author kuiqwang
 * @emai wqkenqingto@163.com
 * @time 2023/2/16
 * @desc
 */
@Slf4j
public class SideProcessFunctionDemo extends ProcessFunction<CarInfo,CarInfo> {
    private static OutputTag littleTag = new OutputTag<CarInfo>("little") {

    };
    private static OutputTag bigtag = new OutputTag<CarInfo>("big") {

    };
    @Override
    public void processElement(CarInfo carInfo, ProcessFunction<CarInfo, CarInfo>.Context context, Collector<CarInfo> collector) throws Exception {
            Integer carNumber = Integer.valueOf(carInfo.getCarNumber().replace("carNo-", ""));
            if (carNumber < 10) {
                context.output(littleTag, carInfo);
            }
            if (carNumber >= 10) {
                context.output(bigtag, carInfo);
            }
        }

}
