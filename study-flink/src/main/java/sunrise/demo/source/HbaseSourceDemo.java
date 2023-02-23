package sunrise.demo.source;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author kuiqwang
 * @emai wqkenqingto@163.com
 * @time 2023/2/22
 * @desc
 */
public class HbaseSourceDemo {
     public static void main(String[] args) throws Exception {
         StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
         env.addSource(new HbaseSource("cs_auth_center")).print();
         env.execute();
    }
}
