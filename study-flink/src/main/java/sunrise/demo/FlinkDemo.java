package sunrise.demo;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import sunrise.reader.MysqlReader;


/**
 * 
 * @author kuiqwang
 * @emai wqkenqingto@163.com
 * @time 2022/11/15
 * @desc 
 */
public class FlinkDemo {
      public static void main(String[] args) throws Exception {
        //
                final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
          String con = "jdbc:mysql://namenode:3306/hy_all";
          String user = "root";
          String password = "Yg123456";
          String sql = "select * from hy_bank";
          String sql2 = "select * from hy_gas_station";
          DataStreamSource<Tuple2<String, String>> stream1 = env.addSource(new MysqlReader(con, user, password, sql)).setParallelism(1);
          DataStreamSource<Tuple2<String, String>> stream2 = env.addSource(new MysqlReader(con, user, password, sql2)).setParallelism(1);
          stream1.join(stream2).where(key -> key.f0).equalTo(key -> key.f0).window(TumblingProcessingTimeWindows.of(Time.seconds(100))).apply((key1, key2) -> {
              System.out.printf("ok");
              return key1.f0 + "\t" + key1.f1 + "\t" + key2.f1;
          }).print().setParallelism(1);
          env.execute();

      }
}
