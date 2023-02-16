package sunrise.demo.window;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import sunrise.demo.function.process.KeyedProcessFunctionDemo;
import sunrise.demo.pojo.Event;
import sunrise.demo.stream.api.source.CustomSource;

/**
 * @author kuiqwang
 * @emai wqkenqingto@163.com
 * @time 2023/2/16
 * @desc
 */
public class KeyedProcessDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.addSource(new CustomSource()).assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
            @Override
            public long extractTimestamp(Event event, long l) {
                return event.getTimestamp();
            }
        })).keyBy(Event::getUser).process(new KeyedProcessFunction<String, Event, String>() {

            @Override
            public void processElement(Event event, KeyedProcessFunction<String, Event, String>.Context context, Collector<String> collector) throws Exception {
                collector.collect("数据到达，时间戳为:" + context.timestamp());
                collector.collect(" 数 据 到 达 ， 水 位 线 为 : " + context.timerService().currentWatermark() + "\n -------分割线-------");
                context.timerService().registerEventTimeTimer(context.timestamp() + 10 * 1000L);
            }

            @Override
            public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                super.onTimer(timestamp, ctx, out);
                out.collect("定时器触发，触发时间:" + timestamp);
            }
        }).print();
        env.execute();
    }
}
