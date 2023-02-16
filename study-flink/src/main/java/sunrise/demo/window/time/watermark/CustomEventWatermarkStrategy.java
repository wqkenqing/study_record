package sunrise.demo.window.time.watermark;

import org.apache.flink.api.common.eventtime.*;
import sunrise.demo.pojo.CarInfo;
import sunrise.demo.pojo.Event;

import java.time.Duration;

/**
 * @author kuiqwang
 * @emai wqkenqingto@163.com
 * @time 2023/2/15
 * @desc 自定义水位线
 */
public class CustomEventWatermarkStrategy implements WatermarkStrategy<Event> {
    @Override
    public WatermarkGenerator<Event> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        return new CustomEventPeriodicGenerator();
    }

    @Override
    public TimestampAssigner<Event> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
        return new SerializableTimestampAssigner<Event>() {
            @Override
            public long extractTimestamp(Event event, long l) {
                return event.getTimestamp();
            }

        };
    }

    @Override
    public WatermarkStrategy<Event> withTimestampAssigner(TimestampAssignerSupplier<Event> timestampAssigner) {
        return WatermarkStrategy.super.withTimestampAssigner(timestampAssigner);
    }

    @Override
    public WatermarkStrategy<Event> withTimestampAssigner(SerializableTimestampAssigner<Event> timestampAssigner) {
        return WatermarkStrategy.super.withTimestampAssigner(timestampAssigner);
    }

    @Override
    public WatermarkStrategy<Event> withIdleness(Duration idleTimeout) {
        return WatermarkStrategy.super.withIdleness(idleTimeout);
    }
}
