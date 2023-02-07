package demo.batch;

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
public class FirstDemo {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> dset = env.fromElements("demo,joe,jack,ken,jack");
        dset.flatMap((String s, Collector<Tuple2<String, Integer>> collect) -> {
            for (String ss : s.split(",")) {
                collect.collect(Tuple2.of(ss, 1));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.INT))
                .groupBy(0).sum(1)
                .print();
    }
}
