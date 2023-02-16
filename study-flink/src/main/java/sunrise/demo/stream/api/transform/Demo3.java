package sunrise.demo.stream.api.transform;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author kuiqwang
 * @emai wqkenqingto@163.com
 * @time 2023/2/15
 * @desc
 */
public class Demo3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> fileStream=env.readTextFile("/Users/kuiqwang/Desktop/gitfiles/study_record/study-flink/data/tableexamples");
        fileStream.map(s -> {
            String[] fields = s.split("\\s+");
            return Tuple2.of(fields[1], 1);
        }).returns(Types.TUPLE(Types.STRING, Types.INT)).keyBy(0).sum(1).print();
        env.execute();
    }
}
