package sunrise.newstream;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import sunrise.demo.source.NumberSource;

import java.util.Arrays;


/**
 * @author kuiqwang
 * @emai wqkenqingto@163.com
 * @time 2025/2/14
 * @desc
 */
public class NewStream1 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 使用 fromCollection 创建数据流
        DataStream<Integer> numberStream = env.fromCollection(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));

        DataStream<Integer> numberStream2 = env.addSource(new NumberSource(), TypeInformation.of(Integer.class));
        numberStream2.union(numberStream);

        env.execute();
    }

}
