package sunrise.demo.util;

import sunrise.demo.annotation.DataT;
import sunrise.demo.annotation.Inject;
import sunrise.demo.pojo.TUser;

import java.lang.reflect.Field;
import java.sql.*;
import java.sql.Date;
import java.util.*;

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

            if (null != map.get(columnName)) {
                Field f = cls.getDeclaredField(map.get(columnName));
                f.setAccessible(true);
                if (type == Types.BIGINT) {
                    f.set(obj, resultSet.getObject(i));
                }
                if (type == Types.VARCHAR) {
                    f.set(obj, resultSet.getString(i));
                }
                if (type == Types.DATE) {
                    f.set(obj, resultSet.getDate(i));
                }
                if (type == Types.TIMESTAMP) {
                    f.set(obj, new Date(resultSet.getTimestamp(columnName).getTime()));
                    //这里是将timestamp 转成了java.sql.Date
//                    f.set(obj, resultSet.getTimestamp(columnName));
                }
            }
        }
        return obj;
    }

    /**
     * 通过类名上的注解进行解析，并反注入生成POJO实例
     */
    public static Object AutoPackageByClassAnntation(Class cls, ResultSet resultSet) throws SQLException, InstantiationException, IllegalAccessException, NoSuchFieldException {
        //创建对象实例
        Object obj = cls.newInstance();

        //获取注解值，即表名
        Inject inject = (Inject) cls.getAnnotation(Inject.class);
//        String tableName = inject.value();
        int colCount = resultSet.getMetaData().getColumnCount();
        for (int i = 1; i <= colCount; i++) {
            String colName = resultSet.getMetaData().getColumnName(i);
            String col = analysisSourceFeildToAttribute(colName);
            Field f = null;
            try {
                f = cls.getDeclaredField(col);
            } catch (Exception e) {
                //不管

            }
            int type = resultSet.getMetaData().getColumnType(i);
            if (null != f) {
                f.setAccessible(true);
                if (type == Types.BIGINT) {
                    f.set(obj, resultSet.getObject(i));
                }
                if (type == Types.VARCHAR) {
                    f.set(obj, resultSet.getString(i));
                }
                if (type == Types.DATE) {
                    f.set(obj, resultSet.getDate(i));
                }
                if (type == Types.TIMESTAMP) {
                    try {
                        if (f != null) {
                            f.set(obj, new Date(resultSet.getTimestamp(colName).getTime()));
                        }
                    } catch (Exception e) {
//                        System.out.println(f.getName());
                    }

                    //这里是将timestamp 转成了java.sql.Date
//                    f.set(obj, resultSet.getTimestamp(columnName));
                }
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

    public static void main(String[] args) throws ClassNotFoundException, SQLException, NoSuchFieldException, InstantiationException, IllegalAccessException {
        Class.forName("com.mysql.cj.jdbc.Driver");//加载数据库驱动
//        connection = DriverManager.getConnection("jdbc:mysql://106.54.170.224:10328", "root", "Bmsoft2020datateam");//获取连接
        String username = "root";
        String password = "Yg123456";
        String dbUrl = "jdbc:mysql://namenode/fkb";
        String device = "t_device";
        String device_department = "daily_report";
        String sql = "select * from t_user";
        Connection connection = DriverManager.getConnection(dbUrl, username, password);//获取连接
        PreparedStatement ps = connection.prepareStatement(sql);
        ResultSet resultSet = ps.executeQuery();
        while (resultSet.next()) {
            TUser dailyReport = (TUser) AutoPackageByClassAnntation(TUser.class, resultSet);
            System.out.println(dailyReport);
        }

    }

    /**
     * 解析字段名并与属性名匹配
     */
    public static String analysisSourceFeildToAttribute(String field) {
        StringBuffer sb = new StringBuffer();
        if (field.contains("_")) {
            String[] fields = field.split("_");
            sb.append(fields[0]);
            for (int a = 1; a <= fields.length - 1; a++) {
                sb.append(fields[a].substring(0, 1).toUpperCase(Locale.ROOT) + fields[a].substring(1));
            }
        } else {
            sb.append(field);
        }
        return sb.toString();
    }

}
