package sunrise.demo.stream.old;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author kuiqwang
 * @emai wqkenqingto@163.com
 * @time 2023/2/8
 * @desc
 */
public class SplitDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataSource = env.socketTextStream("localhost", 8882);
        OutputTag<String> outputTag = new OutputTag<String>("gt"){};
        ProcessFunction<String, String> processFunction = new ProcessFunction<String, String>() {
            @Override
            public void processElement(String s, ProcessFunction<String, String>.Context context, Collector<String> collector) throws Exception {
                int res = Integer.valueOf(s);
                if (res > 10) {
                    System.out.println("this number is big : " + res);
                    collector.collect(String.valueOf(res));
                } else {
                    System.out.println("this number is little : " + res);
                    context.output(outputTag, s);
                }
            }
        };
//        dataSource.process(processFunction).print();
        dataSource.process(processFunction).getSideOutput(outputTag).print();
        env.execute();
    }
}
