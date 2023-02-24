package sunrise.demo.util;

import com.mysql.cj.x.protobuf.MysqlxExpr;
import sunrise.demo.annotation.DataT;
import sunrise.demo.pojo.GoodsRecord;

import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author kuiqwang
 * @emai wqkenqingto@163.com
 * @time 2023/2/24
 * @desc orm小工具，通过反射和注解，实例关系型数据库到java bean的自动数据注入
 */
public class ORMUtil {
    /**
     * 1. 拿到注解信息中val信息
     * 2. 拿到数据类型
     * 3. 创建对对象，并注入
     *
     * @return {database_feild:Object_field}
     */
    public static Map<String, String> getAnnotationVal(Class cls) {
        Map<String, String> anosToFiled = new HashMap<>();
        Field[] field = cls.getDeclaredFields();

        for (Field f : field) {
            anosToFiled.put(f.getAnnotation(DataT.class).value(), f.getName());
        }
        return anosToFiled;
    }

    /**
     * 通过反射map和数据库ResultSet、类对象
     * 进行数据封装
     *
     * @return class的对象
     */
    public static Object autoPackage(Map<String, String> map, ResultSet resultSet, Class cls) throws SQLException, InstantiationException, IllegalAccessException, NoSuchFieldException {
        Object obj = cls.newInstance();
        int counts = resultSet.getMetaData().getColumnCount();

        for (int i = 1; i <= counts; i++) {
            String columnName = resultSet.getMetaData().getColumnName(i);
            int type = resultSet.getMetaData().getColumnType(i);
            Field f = cls.getDeclaredField(columnName);

            f.setAccessible(true);
            if (type == Types.BIGINT) {
                f.set(obj, resultSet.getInt(i));
            }
            if (type == Types.VARCHAR) {
                f.set(obj, resultSet.getString(i));
            }
            if (type == Types.DATE) {
                f.set(obj, resultSet.getDate(i));
            }
        }
        return obj;
    }

    public static List<String> getFieldsFromClass(Class cls) {
        List<String> flist = new ArrayList<>();
        Field[] field = cls.getDeclaredFields();
        for (Field f : field) {
            flist.add(f.getName());
        }
        return flist;
    }
//    public static void main(String[] args) throws ClassNotFoundException, SQLException, NoSuchFieldException, InstantiationException, IllegalAccessException {
//        Class.forName("com.mysql.cj.jdbc.Driver");//加载数据库驱动
////        connection = DriverManager.getConnection("jdbc:mysql://106.54.170.224:10328", "root", "Bmsoft2020datateam");//获取连接
//        String username = "root";
//        String password = "Yg123456";
//        String dbUrl = "jdbc:mysql://namenode/fkb";
//        String device = "t_device";
//        String device_department = "t_device_department";
//        String sql = "select * from goods_record";
//        Connection connection = DriverManager.getConnection(dbUrl, username, password);//获取连接
//        PreparedStatement ps = connection.prepareStatement(sql);
//        ResultSet resultSet = ps.executeQuery();
//        while (resultSet.next()) {
//            Map<String, String> res = getAnnotationVal(GoodsRecord.class);
//            GoodsRecord goodsRecord = (GoodsRecord) autoPackage(res, resultSet, GoodsRecord.class);
//            System.out.println(goodsRecord);
//        }
//    }

}
