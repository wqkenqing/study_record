package sunrise.demo.window.time.watermark;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import sunrise.demo.pojo.CarInfo;

/**
 * @author kuiqwang
 * @emai wqkenqingto@163.com
 * @time 2023/2/15
 * @desc
 */
public class CustomPeriodicGenerator implements WatermarkGenerator<CarInfo> {
    private Long delayTime = 5000L; // 延迟时间
    private Long maxTs = Long.MIN_VALUE + delayTime + 1L; // 观察到的最大时间戳

    @Override
    public void onEvent(CarInfo carInfo, long l, WatermarkOutput watermarkOutput) {
        maxTs = Math.max(carInfo.getEventTime(), maxTs); // 更新最大时间戳
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
        watermarkOutput.emitWatermark(new Watermark(maxTs - delayTime - 1L));
    }
}
