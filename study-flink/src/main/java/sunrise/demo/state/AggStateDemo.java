package sunrise.demo.state;

import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import sunrise.demo.function.agg.AvgAggStateFuntion;
import sunrise.demo.pojo.CarInfo;

/**
 * @author kuiqwang
 * @emai wqkenqingto@163.com
 * @time 2023/2/10
 * @desc
 */
public class AggStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> carInfo = env.socketTextStream("localhost", 8883);
        //车速信息算平均值
        carInfo.map(s -> {
            JSONObject carObj = JSONObject.parse(s);
            CarInfo car = new CarInfo();
            car.setCarNumber((String) carObj.get("carNumber"));
            car.setCarSpeed((Integer) carObj.get("carSpeed"));
            return car;
        }).keyBy(CarInfo::getCarNumber).process(new AvgAggStateFuntion()).print();
        env.execute();
    }
}
