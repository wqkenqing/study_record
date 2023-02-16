package sunrise.demo.stream.api.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import sunrise.demo.pojo.Event;

/**
 * @author kuiqwang
 * @emai wqkenqingto@163.com
 * @time 2023/2/15
 * @desc
 */
public class AutoEventSourceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        DataStreamSource<Event> eventSource = env.addSource(new AutoEventSource());
        eventSource.print();
        env.execute();
    }
}
