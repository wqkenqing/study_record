package sunrise.demo.stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import sunrise.demo.function.process.ProcessFunctionDemo;
import sunrise.demo.pojo.CarInfo;

/**
 * @author kuiqwang
 * @emai wqkenqingto@163.com
 * @time 2023/2/16
 * @desc 测试自定义Process函数，这里是测试了process函数，实现效果是通过process函数，过滤车牌尾号大于55和车速超过80的车辆信息
 */
public class CustomProcessStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> customStream = env.socketTextStream("localhost", 8883);
        customStream.map(s -> {
            ObjectMapper mapper = new ObjectMapper();
            return mapper.readValue(s, CarInfo.class);
        }).process(new ProcessFunctionDemo()).print();
        env.execute();
    }
}
