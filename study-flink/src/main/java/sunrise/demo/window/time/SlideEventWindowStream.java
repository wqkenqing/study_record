package sunrise.demo.window.time;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import sunrise.demo.pojo.CarInfo;

import javax.annotation.Nullable;

/**
 * @author kuiqwang
 * @emai wqkenqingto@163.com
 * @time 2023/2/15
 * @desc
 */
public class SlideEventWindowStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> carInfo = env.socketTextStream("localhost", 8883);
        carInfo.map((String s) -> {
            ObjectMapper mapper = new ObjectMapper();
            CarInfo info = mapper.readValue(s, CarInfo.class);
            return info;
        }).assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<CarInfo>() {
            private long curruntTime;

            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(curruntTime);
            }

            @Override
            public long extractTimestamp(CarInfo carInfo, long l) {
                long time = carInfo.getEventTime();
                curruntTime = Math.max(curruntTime, time);
                return curruntTime;
            }
        }).keyBy(CarInfo::getCarNumber).timeWindow(Time.seconds(5), Time.seconds(2)).reduce((c1, c2) -> {
            c1.setCarSpeed(c1.getCarSpeed() + c2.getCarSpeed());
            return c1;
        }).print();
        env.execute();
    }
}
