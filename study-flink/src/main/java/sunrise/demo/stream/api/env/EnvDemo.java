package sunrise.demo.stream.api.env;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import scala.tools.nsc.transform.patmat.MatchAnalysis;

/**
 * @author kuiqwang
 * @emai wqkenqingto@163.com
 * @time 2023/2/15
 * @desc
 */
public class EnvDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        DataStreamSource<String> textSource = env.readTextFile("/Users/kuiqwang/Desktop/gitfiles/study_record/study-flink/data/carId2Name");
        textSource.map(s -> {
            String ss[] = s.split("\\s+");
            return Tuple2.of(ss[0], ss[1]);
        }).returns(Types.TUPLE(Types.STRING, Types.STRING)).print();
        env.execute();
    }
}
