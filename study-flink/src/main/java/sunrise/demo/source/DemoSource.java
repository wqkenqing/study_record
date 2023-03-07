package sunrise.demo.source;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import sunrise.demo.pojo.NcpDetail;

/**
 * @author kuiqwang
 * @emai wqkenqingto@163.com
 * @time 2023/2/28
 * @desc
 */
public class DemoSource extends RichParallelSourceFunction   {


    @Override
    public void run(SourceContext sourceContext) throws Exception {

    }

    @Override
    public void cancel() {

    }
}
