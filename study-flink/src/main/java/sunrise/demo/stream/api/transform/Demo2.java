package sunrise.demo.stream.api.transform;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author kuiqwang
 * @emai wqkenqingto@163.com
 * @time 2023/2/15
 * @desc
 */
public class Demo2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> fileStream=env.readTextFile("/Users/kuiqwang/Desktop/gitfiles/study_record/study-flink/data/tableexamples");
        fileStream.flatMap((String s, Collector<String> out) -> {
            String[] ss = s.split("\\s+");
            for (String word : ss) {
                out.collect(word);
            }
        }).returns(Types.STRING).filter(s -> s.length() > 2).print();
        env.execute();
    }
}
