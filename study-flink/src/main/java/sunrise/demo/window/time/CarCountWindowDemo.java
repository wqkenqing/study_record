package sunrise.demo.window.time;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import sunrise.demo.function.window.CarCountProcessWindowFunction;
import sunrise.demo.pojo.CarInfo;

/**
 * @author kuiqwang
 * @emai wqkenqingto@163.com
 * @time 2023/2/16
 * @desc
 */
public class CarCountWindowDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> carStream = env.socketTextStream("localhost", 8883);
        carStream.map(s -> {
                    ObjectMapper mapper = new ObjectMapper();
                    return mapper.readValue(s, CarInfo.class);
                }).returns(TypeInformation.of(CarInfo.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<CarInfo>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<CarInfo>() {
                    @Override
                    public long extractTimestamp(CarInfo carInfo, long l) {
                        return carInfo.getEventTime();
                    }
                }))
                .keyBy(CarInfo::getCarNumber).window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .reduce((s1,s2)->{
                    s1.setCarSpeed(s1.getCarSpeed() + s2.getCarSpeed());
                    return s1;
                },new CarCountProcessWindowFunction()).print();
        env.execute();
    }
}
