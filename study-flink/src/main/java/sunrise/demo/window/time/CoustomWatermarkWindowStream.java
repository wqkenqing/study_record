package sunrise.demo.window.time;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import sunrise.demo.pojo.CarInfo;
import sunrise.demo.window.time.watermark.CustomWatermarkStrategy;

import javax.annotation.Nullable;

/**
 * @author kuiqwang
 * @emai wqkenqingto@163.com
 * @time 2023/2/15
 * @desc
 */
public class CoustomWatermarkWindowStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> carInfo = env.socketTextStream("localhost", 8883);
        carInfo.map((String s) -> {
            ObjectMapper mapper = new ObjectMapper();
            CarInfo info=mapper.readValue(s, CarInfo.class);
            return info;
        }).assignTimestampsAndWatermarks(new CustomWatermarkStrategy() {
        }).keyBy(CarInfo::getCarNumber).window(TumblingEventTimeWindows.of(Time.seconds(5))).max("carSpeed").print();
        env.execute();
    }
}
