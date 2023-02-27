package run;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

/**
 * @author kuiqwang
 * @emai wqkenqingto@163.com
 * @time 2023/2/24
 * @desc
 */
public class AutoDemo {

    public static void getClassObject(Class cls) {
        List<String> anos = new ArrayList<>();
        Field[] field = cls.getDeclaredFields();

        for (Field f : field) {
            anos.add(f.getAnnotation(DataT.class).value());
        }
    }

    public static void main(String[] args) throws ClassNotFoundException {
        Class<?> cls = Class.forName("run.Student");
        getClassObject(cls);
    }
}
