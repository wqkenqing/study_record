package sunrise.reader;/**
 * 
 * @author kuiqwang
 * @emai wqkenqingto@163.com
 * @time 2022/11/15
 * @desc 
 */
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class MysqlReader extends RichSourceFunction<Tuple2<String, String>> {


    private Connection connection = null;
    private PreparedStatement ps = null;
     String con;
     String user;
     String pass;
     String sql;
    public MysqlReader(String con,String user,String pass,String sql ){
        this.user=user;
        this.pass=pass;
        this.con=con;
        this.sql=sql;
    }
    //该方法主要用于打开数据库连接，下面的ConfigKeys类是获取配置的类
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Class.forName("com.mysql.cj.jdbc.Driver");//加载数据库驱动
//        connection = DriverManager.getConnection("jdbc:mysql://106.54.170.224:10328", "root", "Bmsoft2020datateam");//获取连接

        connection = DriverManager.getConnection(con, user, pass);//获取连接
        ps = connection.prepareStatement(sql);
    }


    @Override
    public void run(SourceContext<Tuple2<String, String>> sourceContext) throws Exception {
        ResultSet resultSet = ps.executeQuery();
        while (resultSet.next()) {
            Tuple2<String, String> tuple = new Tuple2<String, String>();
            tuple.setFields(resultSet.getString(1), resultSet.getString(2));
            sourceContext.collect(tuple);
        }
    }

    @Override
    public void cancel() {
        try {
            super.close();
            if (connection != null) {
                connection.close();
            }
            if (ps != null) {
                ps.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
