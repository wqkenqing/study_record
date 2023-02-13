package sunrise.demo.window;

import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.shaded.jackson2.org.yaml.snakeyaml.events.Event;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import sunrise.demo.pojo.CarInfo;

import java.time.Duration;


/**
 * @author kuiqwang
 * @emai wqkenqingto@163.com
 * @time 2023/2/13
 * @desc
 */
public class TumblingWindowDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketStream = env.socketTextStream("", 8883);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //ProcessTimeWindow
//        socketStream.map(s -> {
//            CarInfo info = new CarInfo();
//            JSONObject carObj = JSONObject.parse(s);
//            info.setCarNumber(carObj.getString("carNumber"));
//            info.setEventTime(carObj.getLong("eventTime"));
//            info.setCarSpeed(carObj.getInteger("carSpeed"));
//            return info;
//        }).keyBy(CarInfo::getCarNumber).window(TumblingProcessingTimeWindows.of(Time.seconds(5))).reduce((s1, s2)->{
//            s1.setCarSpeed(s1.getCarSpeed() + s2.getCarSpeed());
//            return s1;
//        }).print();
        //EventTimeWindow
        socketStream.map(s -> {
            CarInfo info = new CarInfo();
            JSONObject carObj = JSONObject.parse(s);
            info.setCarNumber(carObj.getString("carNumber"));
            info.setEventTime(carObj.getLong("eventTime"));
            info.setCarSpeed(carObj.getInteger("carSpeed"));
            return info;
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<CarInfo>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<CarInfo>() {
            @Override
            public long extractTimestamp(CarInfo carInfo, long l) {
                return carInfo.getEventTime();
            }
        })).keyBy(CarInfo::getCarNumber)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce((s1, s2)->{
            s1.setCarSpeed(s1.getCarSpeed() + s2.getCarSpeed());
            return s1;
        }).print();
        env.execute();
    }
}
