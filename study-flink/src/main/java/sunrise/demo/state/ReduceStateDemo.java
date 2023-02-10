package sunrise.demo.state;

import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import sunrise.demo.function.CarReduceFuntion;
import sunrise.demo.function.WordFlatMapFunction;
import sunrise.demo.pojo.CarInfo;

/**
 * @author kuiqwang
 * @emai wqkenqingto@163.com
 * @time 2023/2/9
 * @desc
 */
public class ReduceStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> carReduceStream = env.socketTextStream("localhost", 8883);
        carReduceStream.map(s -> {
            CarInfo carInfo = new CarInfo();
            JSONObject carObj = JSONObject.parse(s);
            String carNumber = (String) carObj.get("carNumber");
            Integer carSpeed = (Integer) carObj.get("carSpeed");
            carInfo.setCarSpeed(carSpeed);
            carInfo.setCarNumber(carNumber);
            return carInfo;
        }).keyBy(CarInfo::getCarNumber).process(new CarReduceFuntion()).print();
        env.execute();
    }
}
