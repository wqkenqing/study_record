package sunrise.demo.window.time;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import sunrise.demo.function.agg.AvgEventUrlAggFunticon;
import sunrise.demo.pojo.Event;
import sunrise.demo.stream.api.source.AutoEventSource;
import sunrise.demo.window.time.watermark.CustomEventWatermarkStrategy;

/**
 * @author kuiqwang
 * @emai wqkenqingto@163.com
 * @time 2023/2/15
 * @desc
 */
public class SessionWindowDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Event> eventSource=env.addSource(new AutoEventSource());
        eventSource.assignTimestampsAndWatermarks(new CustomEventWatermarkStrategy()).keyBy(Event::getUser)
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(10))).aggregate(new AvgEventUrlAggFunticon()).print();
        env.execute();
    }
}
