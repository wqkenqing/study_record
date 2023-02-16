package sunrise.demo.stream.api.source;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import sunrise.demo.pojo.Event;

import java.util.Calendar;
import java.util.Random;

/**
 * @author kuiqwang
 * @emai wqkenqingto@163.com
 * @time 2023/2/15
 * @desc
 */
public class AutoEventSource implements ParallelSourceFunction<Event> {
    private Boolean running = true;

    @Override
    public void run(SourceContext<Event> sourceContext) throws Exception {
        String[] users = {"Mary", "Alice", "Bob", "Cary"};
        String[] urls = {"./home", "./cart", "./fav", "./prod?id=1", "./prod?id=2"};
        Random random = new Random();
        while (running) {
            Event event = new Event(
                    users[random.nextInt(users.length)],
                    urls[random.nextInt(urls.length)],
                    Calendar.getInstance().getTimeInMillis());
            //发送水位线
            sourceContext.collect(event);
            sourceContext.collectWithTimestamp(event, event.getTimestamp());
            sourceContext.emitWatermark(new Watermark(event.getTimestamp()-1l));
// 隔 1 秒生成一个点击事件，方便观测
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
