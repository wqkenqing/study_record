package sunrise.demo.stream.old;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.co.RichCoMapFunction;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;

import java.util.HashMap;

/**
 * @author kuiqwang
 * @emai wqkenqingto@163.com
 * @time 2023/2/8
 * @desc
 */
public class CoDemo {
    final static HashMap carID = new HashMap();

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> carIdSource = env.readTextFile("/Users/kuiqwang/Desktop/gitfiles/study_record/study-flink/src/main/resources/carFlow_all_column_test.txt").setParallelism(1);
        DataStreamSource<String> socketSource = env.socketTextStream("0.0.0.0", 8882).setParallelism(1);
        CoMapFunction<String, String, String> carCoMapFunction = new RichCoMapFunction<String, String, String>() {
            @Override
            public String map1(String s) throws Exception {
                System.out.printf("carID Map的长度是：" + carID.size());

                System.out.printf("this is res:" + s);
                if (carID.get(s) != null) {
                    String result = (String) carID.get(s);
                    System.out.println("result is : " + result);
                    return result;
                }
                return "this is null";
            }

            @Override
            public String map2(String s) throws Exception {
                String[] infos = s.split(",");
                String carId = infos[2].replace("\'", "");
                String res = infos[2];
                carID.put(carId, res);
                System.out.println("carId's size is :" + carID.entrySet().size());
                return res;
            }

        };
        socketSource.connect(carIdSource).map(carCoMapFunction).broadcast().print();
        env.execute();
    }
}
