package sunrise.demo.window;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SessionWindowTimeGapExtractor;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter;
import org.codehaus.jackson.map.ObjectMapper;
import sunrise.demo.pojo.CarInfo;

import java.time.Duration;

/**
 * @author kuiqwang
 * @emai wqkenqingto@163.com
 * @time 2023/2/15
 * @desc
 */
public class DynamicSessionWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> carStream = env.socketTextStream("localhost", 8883);
        carStream.map(s -> {
            ObjectMapper objectMapper = new ObjectMapper();
            return objectMapper.readValue(s, CarInfo.class);
        })
                .returns(TypeInformation.of(CarInfo.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<CarInfo>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<CarInfo>() {
                    @Override
                    public long extractTimestamp(CarInfo carInfo, long l) {
                        return carInfo.getEventTime();
                    }
                })).keyBy(CarInfo::getCarNumber).window(ProcessingTimeSessionWindows.withDynamicGap(new SessionWindowTimeGapExtractor<CarInfo>() {
            @Override
            public long extract(CarInfo carInfo) {
                return carInfo.getCarNumber().length() * 1000;
            }
        })).sum("carSpeed").print();
        env.execute();
    }
}
