package sunrise.demo.stream.api.transform;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import sunrise.demo.pojo.Event;
import sunrise.demo.stream.api.source.AutoEventSource;

/**
 * @author kuiqwang
 * @emai wqkenqingto@163.com
 * @time 2023/2/15
 * @desc
 */
public class Demo1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Event> eventSource = env.addSource(new AutoEventSource());
        eventSource.map(event -> {
            return Tuple2.of(event.getUser(), event.getUrl());
        }).returns(Types.TUPLE(Types.STRING, Types.STRING)).print();
        env.execute();
    }
}
