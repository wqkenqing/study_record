package sunrise.demo.window.time.watermark;

import org.apache.flink.api.common.eventtime.*;
import sunrise.demo.pojo.CarInfo;

import java.time.Duration;

/**
 * @author kuiqwang
 * @emai wqkenqingto@163.com
 * @time 2023/2/15
 * @desc 自定义水位线
 */
public class CustomWatermarkStrategy implements WatermarkStrategy<CarInfo> {
    @Override
    public WatermarkGenerator<CarInfo> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        return new CustomPeriodicGenerator();
    }

    @Override
    public TimestampAssigner<CarInfo> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
        return new SerializableTimestampAssigner<CarInfo>() {
            @Override
            public long extractTimestamp(CarInfo carInfo, long l) {
                return carInfo.getEventTime();
            }
        };
    }

    @Override
    public WatermarkStrategy<CarInfo> withTimestampAssigner(TimestampAssignerSupplier<CarInfo> timestampAssigner) {
        return WatermarkStrategy.super.withTimestampAssigner(timestampAssigner);
    }

    @Override
    public WatermarkStrategy<CarInfo> withTimestampAssigner(SerializableTimestampAssigner<CarInfo> timestampAssigner) {
        return WatermarkStrategy.super.withTimestampAssigner(timestampAssigner);
    }

    @Override
    public WatermarkStrategy<CarInfo> withIdleness(Duration idleTimeout) {
        return WatermarkStrategy.super.withIdleness(idleTimeout);
    }
}
