package sunrise.demo.stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import sunrise.demo.function.process.SideProcessFunctionDemo;
import sunrise.demo.pojo.CarInfo;

/**
 * @author kuiqwang
 * @emai wqkenqingto@163.com
 * @time 2023/2/16
 * @desc 测试旁路输出
 */
public class SideOutputStreamDemo {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> carStream = env.socketTextStream("localhost", 8883);
        SingleOutputStreamOperator<CarInfo> sideCar= carStream.map(s -> {
            ObjectMapper mapper = new ObjectMapper();
            return mapper.readValue(s, CarInfo.class);
        }).returns(TypeInformation.of(CarInfo.class)).process(new SideProcessFunctionDemo());
        OutputTag<CarInfo> littleTag = new OutputTag<CarInfo>("little") {

        };
        OutputTag<CarInfo> bigtag = new OutputTag<CarInfo>("big") {
        };
//        sideCar.getSideOutput(littleTag).print("little-number");
//        sideCar.getSideOutput(bigtag).print("big-number");
        env.execute();
    }
}
