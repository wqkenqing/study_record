package sunrise.demo.window;

import com.alibaba.fastjson2.JSONObject;
import lombok.extern.slf4j.Slf4j;
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
 * @time 2023/2/13
 * @desc
 */
@Slf4j
public class TumblingWindowStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketStream = env.socketTextStream("localhost", 8883);
//        socketStream
//                .map(s -> Tuple2.of(s, 1)).returns(Types.TUPLE(Types.STRING, Types.INT))
//                .keyBy(0)
//                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
//                .sum(1).print();
        socketStream
                .map(s -> {
                    CarInfo info = new CarInfo();
                    JSONObject carObj = JSONObject.parse(s);
                    info.setCarSpeed((Integer) carObj.get("carSpeed"));
                    info.setCarNumber((String) carObj.get("carNumber"));
                    info.setEventTime((Long) carObj.get("eventTime"));
                    return info;
                }).assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<CarInfo>() {
                    private long currentTimestamp;

                    @Override
                    public long extractTimestamp(CarInfo carInfo, long l) {
                        long timestamp = carInfo.getEventTime();
                        currentTimestamp = Math.max(timestamp, currentTimestamp);
                        return timestamp;
                    }

                    @Nullable
                    @Override
                    public Watermark getCurrentWatermark() {
                        return new Watermark(currentTimestamp);
                    }
                }).keyBy(CarInfo::getCarNumber).window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce((s1, s2) -> {
                    CarInfo info = new CarInfo();
                    info.setCarSpeed(s1.getCarSpeed() + s2.getCarSpeed());
                    info.setCarNumber(s1.getCarNumber());
                    return info;
                }).print();
        env.execute();
    }
}
