package sunrise.demo.source;/**
 * @author kuiqwang
 * @emai wqkenqingto@163.com
 * @time 2022/11/15
 * @desc
 */

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import sunrise.demo.util.ORMUtil;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class MysqlReaderNew extends RichSourceFunction<Object> {


    String con;
    String user;
    String pass;
    String sql;
    Class cls;
    private Connection connection = null;
    private PreparedStatement ps = null;

    public MysqlReaderNew(String con, String user, String pass, String sql, Class cls) {
        this.user = user;
        this.pass = pass;
        this.con = con;
        this.sql = sql;
        this.cls = cls;
    }

    //该方法主要用于打开数据库连接，下面的ConfigKeys类是获取配置的类
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Class.forName("com.mysql.cj.jdbc.Driver");//加载数据库驱动
//        connection = DriverManager.getConnection("jdbc:mysql://106.54.170.224:10328", "root", "Bmsoft2020datateam");//获取连接
        if (connection == null) {
            connection = DriverManager.getConnection(con, user, pass);//获取连接
        }
        ps = connection.prepareStatement(sql);
    }


    @Override
    public void run(SourceContext<Object> sourceContext) throws Exception {
        ResultSet resultSet = ps.executeQuery();
        while (resultSet.next()) {
            sourceContext.collect(ORMUtil.AutoPackageByClassAnntation(cls, resultSet));
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
