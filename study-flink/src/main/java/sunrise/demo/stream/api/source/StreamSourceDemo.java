package sunrise.demo.stream.api.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * @author kuiqwang
 * @emai wqkenqingto@163.com
 * @time 2023/2/15
 * @desc flink 常规source 的熟悉
 */
public class StreamSourceDemo {
        public static void main(String[] args) throws Exception {
                StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
                List<String> collecList = new ArrayList<>();
                for (int i = 0; i <100 ; i++) {
                        String carNumber = "car_number";
                        carNumber += i;
                        collecList.add(carNumber);
                }
                //从collection中读取
//                DataStreamSource<String> cStream = env.fromCollection(collecList);
                //                cStream.print();
                //从文件中读取
//                DataStreamSource<String> fSteam = env.readTextFile("/Users/kuiqwang/Desktop/gitfiles/study_record/study-flink/data/id2city");
                //从hdfs地址读取
//                DataStreamSource<String> fSteam = env.readTextFile("hdfs://scm-server:8020/hy_bank.txt");
//
//                fSteam.print();
                //从socket中读取数据
//                DataStreamSource<String> socketStream = env.socketTextStream("localhost", 8882);
//                socketStream.print();
                env.execute();
        }
}
