package sunrise.demo.state;

import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import sunrise.demo.pojo.CarInfo;

/**
 * @author kuiqwang
 * @emai wqkenqingto@163.com
 * @time 2023/2/9
 * @desc
 */
public class MapStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> carInfoStream = env.socketTextStream("localhost", 8883);
        KeySelector<CarInfo, String> carKey = new KeySelector<CarInfo, String>() {
            @Override
            public String getKey(CarInfo carObj) throws Exception {
                return carObj.getCarNumber();
            }
        };
        carInfoStream.map(s -> {
            CarInfo info = new CarInfo();
            JSONObject carObj = JSONObject.parse(s);
            info.setCarNumber((String) carObj.get("carNumber"));
            info.setCarSpeed((Integer) carObj.get("carSpeed"));
            return info;
        }).keyBy(carKey).max("carSpeed").print();
        env.execute();
    }
}
