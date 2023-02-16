package sunrise.demo.state;

import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import sunrise.demo.function.agg.CarReduceFuntion;
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
