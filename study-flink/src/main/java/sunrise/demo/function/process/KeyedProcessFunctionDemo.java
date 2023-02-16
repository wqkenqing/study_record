package sunrise.demo.function.process;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import sunrise.demo.pojo.Event;

/**
 * @author kuiqwang
 * @emai wqkenqingto@163.com
 * @time 2023/2/16
 * @desc
 */
@Slf4j
public class KeyedProcessFunctionDemo extends KeyedProcessFunction<Boolean, Event, String> {

    @Override
    public void processElement(Event event, KeyedProcessFunction<Boolean, Event, String>.Context context, Collector<String> collector) throws Exception {
        collector.collect("数据到达，时间戳为:" + context.timestamp());
        collector.collect(" 数 据 到 达 ， 水 位 线 为 : " + context.timerService().currentWatermark() + "\n -------分割线-------");
        context.timerService().registerEventTimeTimer(context.timestamp() + 10 * 1000L);
    }

    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<Boolean, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
        super.onTimer(timestamp, ctx, out);
        out.collect("定时器触发，触发时间:" + timestamp);
    }
}
