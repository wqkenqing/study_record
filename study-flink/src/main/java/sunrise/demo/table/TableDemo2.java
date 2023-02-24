package sunrise.demo.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author kuiqwang
 * @emai wqkenqingto@163.com
 * @time 2023/2/24
 * @desc
 */
public class TableDemo2 {

    public static void main(String[] args) throws Exception {
        // 基于StreamExecutionEnvironment创建TableEnvironment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
//        tableEnv.useCatalog("custom_catalog");
//        tableEnv.useCatalog("custom_database");
        String username = "root";
        String password = "Yg123456";
        String dbUrl = "jdbc:mysql://namenode/fkb";
        String device = "t_device";
        String device_department = "t_device_department";
        String query = String.format("CREATE TABLE %s (\n" +
                "  id BigInt,\n" +
                "  identification STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'jdbc',\n" +
                "  'url' = '%s',\n" +
                "  'table-name' = '%s',\n" +
                "  'username' = '%s',\n" +
                "  'password' = '%s'\n" +
                ")", device, dbUrl, device, username, password);
        String query2 = String.format("CREATE TABLE %s (\n" +
                "  device_id BigInt,\n" +
                "  department_id BigInt\n" +
                ") WITH (\n" +
                "  'connector' = 'jdbc',\n" +
                "  'url' = '%s',\n" +
                "  'table-name' = '%s',\n" +
                "  'username' = '%s',\n" +
                "  'password' = '%s'\n" +
                ")", device_department, dbUrl, device_department, username, password);
        String query3 = String.format("CREATE TABLE %s (\n" +
                "  id BigInt,\n" +
                "  department_id BigInt\n" +
                ") WITH (\n" +
                "  'connector' = 'jdbc',\n" +
                "  'url' = '%s',\n" +
                "  'table-name' = '%s',\n" +
                "  'username' = '%s',\n" +
                "  'password' = '%s'\n" +
                ")", device_department, dbUrl, device_department, username, password);

        tableEnv.executeSql(query);
        tableEnv.executeSql(query2);
        Table result = tableEnv.sqlQuery("select * from t_device t join t_device_department d on t.id=d.device_id ");

//        Table result2 = tableEnv.sqlQuery("select * from department");
        result.execute().print();
//        tableEnv.toDataStream(result).print();
        env.execute();
    }
}
