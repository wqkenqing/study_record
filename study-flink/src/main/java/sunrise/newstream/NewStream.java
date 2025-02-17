package sunrise.newstream;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.api.common.typeinfo.Types;
import sunrise.demo.pojo.CarInfo;

public class NewStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 从 Socket 读取数据流
        DataStreamSource<String> socketTextStream = env.socketTextStream("localhost", 8883);

        socketTextStream
                // 将输入的 JSON 字符串转换为 Tuple3(carNumber, carSpeed, 1)
                .map(s -> {
                    CarInfo carInfo = new ObjectMapper().readValue(s, CarInfo.class);
                    return Tuple3.of(carInfo.getCarNumber(), Double.valueOf(carInfo.getCarSpeed()), 1);
                })
                .returns(Types.TUPLE(Types.STRING, Types.DOUBLE, Types.INT)) // 明确返回类型
                // 给每个事件分配时间戳和水印
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple3<String, Double, Integer>>() {
                    private long currentTimestamp = 0;

                    @Override
                    public long extractTimestamp(Tuple3<String, Double, Integer> element, long previousElementTimestamp) {
                        // 你可以从 carInfo 中提取时间戳（假设 carInfo 中有时间戳字段）
                        long timestamp = System.currentTimeMillis();  // 使用当前时间戳，或者根据需要提取时间戳
                        currentTimestamp = timestamp;
                        return timestamp;
                    }

                    @Override
                    public Watermark getCurrentWatermark() {
                        // 返回水印，通常使用当前时间戳减去延迟
                        return new Watermark(currentTimestamp - 1000);  // 假设延迟最大为 1 秒
                    }
                })
                .keyBy(t -> t.f0) // 按照车号分组
                .window(TumblingProcessingTimeWindows.of(Time.minutes(3))) // 设置每 10 秒一个时间窗口
                .reduce(new ReduceFunction<Tuple3<String, Double, Integer>>() {
                    @Override
                    public Tuple3<String, Double, Integer> reduce(Tuple3<String, Double, Integer> carOld, Tuple3<String, Double, Integer> carNew) throws Exception {
                        // 累加总速度和事件数
                        return Tuple3.of(carOld.f0, carOld.f1 + carNew.f1, carOld.f2 + carNew.f2);
                    }
                })
                // 在窗口结束时计算平均车速
                .map(tuple -> {
                    return Tuple3.of(tuple.f0, tuple.f1 / tuple.f2,tuple.f2); // 返回车号和平均车速
                })
                .returns(Types.TUPLE(Types.STRING, Types.DOUBLE,Types.INT)) // 正确的返回类型
                .print(); // 打印每辆车的平均速度

        env.execute("Car Speed Average Calculation");
    }
}
