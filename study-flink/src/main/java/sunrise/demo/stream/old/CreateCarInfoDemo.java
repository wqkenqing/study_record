package sunrise.demo.stream.old;

import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * @author kuiqwang
 * @emai wqkenqingto@163.com
 * @time 2023/2/9
 * @desc
 */
public class CreateCarInfoDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> carSourceInfo = env.socketTextStream("localhost", 8882);
        carSourceInfo.map(s -> JSONObject.toJSONString(JSONObject.of(s, Math.random() * 100))).writeToSocket("localhost", 8883, new SimpleStringSchema());
        env.execute();
    }
}
