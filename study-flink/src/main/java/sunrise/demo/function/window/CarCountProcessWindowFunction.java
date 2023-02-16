package sunrise.demo.function.window;

import org.apache.commons.net.ntp.TimeStamp;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import scala.collection.Iterable;
import scala.collection.immutable.List;
import sunrise.demo.pojo.CarInfo;

import java.sql.Timestamp;
import java.util.HashSet;
import java.util.Set;

/**
 * @author kuiqwang
 * @emai wqkenqingto@163.com
 * @time 2023/2/16
 * @desc
 */
public class CarCountProcessWindowFunction extends ProcessWindowFunction<CarInfo,String,String, TimeWindow> {


    @Override
    public void process(String s, ProcessWindowFunction<CarInfo, String, String, TimeWindow>.Context context, java.lang.Iterable<CarInfo> iterable, Collector<String> collector) throws Exception {
        Set<String> carSet = new HashSet<String>();
        for (CarInfo car : iterable) {
            carSet.add(car.getCarNumber());
        }
        long start = context.window().getStart();
        long end = context.window().getEnd();

        collector.collect("窗口:" + new Timestamp(start) + "~" + new Timestamp(end) + "的过车数是：" + carSet.size());

    }
}
