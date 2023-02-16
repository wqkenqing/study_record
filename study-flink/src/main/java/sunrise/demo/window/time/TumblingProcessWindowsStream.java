package sunrise.demo.window.time;

import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import sunrise.demo.pojo.CarInfo;

/**
 * @author kuiqwang
 * @emai wqkenqingto@163.com
 * @time 2023/2/15
 * @desc
 */
public class TumblingProcessWindowsStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> carInfo = env.socketTextStream("localhost", 8883);
        carInfo.map(s -> {
            JSONObject carObj = JSONObject.parse(s);
            CarInfo info = new CarInfo();
            info.setCarSpeed(carObj.getInteger("carSpeed"));
            info.setCarNumber(carObj.getString("carNumber"));
            info.setEventTime(carObj.getLong("eventTime"));
            return info;
        }).returns(TypeInformation.of(CarInfo.class)).keyBy(CarInfo::getCarSpeed).window(TumblingProcessingTimeWindows.of(Time.seconds(5))).max("carSpeed").print();
        env.execute();
    }
}
