package sunrise.demo.batch;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author kuiqwang
 * @emai wqkenqingto@163.com
 * @time 2023/2/7
 * @desc
 */
public class Demo2 {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> dset = env.readTextFile("/Users/kuiqwang/Desktop/tengxun_class/study-flink/src/main/resources/carFlow_all_column_test.txt");
        dset.map(s -> {
            return Tuple2.of(s.split(",")[0], 1);
        }).returns(Types.TUPLE(Types.STRING, Types.INT)).groupBy(0).sum(1).sortPartition(0, Order.ASCENDING).print();

    }
}
