package sunrise.demo.source;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

/**
 * @author kuiqwang
 * @emai wqkenqingto@163.com
 * @time 2025/2/14
 * @desc
 */
public class NumberSource extends RichSourceFunction<Integer> {
    @Override
    public void run(SourceContext<Integer> sourceContext) throws Exception {
        for (int i = 0; i < 1000; i++) {
            Integer number = (int) (Math.random() * 1000);
            sourceContext.collect(number);
        }
    }

    @Override
    public void cancel() {

    }
}
