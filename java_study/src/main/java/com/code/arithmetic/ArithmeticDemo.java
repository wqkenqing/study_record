package com.code.arithmetic;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * @author kuiqwang
 * @emai wqkenqingto@163.com
 * @time 2023/2/20
 * @desc 计算处理总结
 */
public class ArithmeticDemo {
/**
 * 计算处理有 加、减、乘、除
 * 取mod。保留几位小数等
 * */
    public static void normalOperationMethod() {

        int a = -5;
        int b = 2;
        int sum = a + b; // addition
        int difference = a - b; // subtraction
        int product = a * b; // multiplication
        int quotient = a / b; // division
        System.out.println(a % b);
    }

    /**
     * math class
     */
    public static void mathMethod() {
        int a =-5;
        int b = 2;
        System.out.println(Math.abs(a));
        System.out.println(Math.max(a, b));
        System.out.println(Math.min(a, b));
        System.out.println(Math.sqrt(2));
        System.out.println(Math.subtractExact(b,a));
        System.out.println(Math.pow(b,a));
        System.out.println(Math.random());
        System.out.println(Math.floorDiv(a,b));
        System.out.println(Math.floorMod(a,b));
        System.out.println(a - b * (a / b));
        System.out.println(-5/2);

    }

    public static void BigDecimalMethod() {
        double val = 4.23;
        double val1 = 5.23;
        BigDecimal decimal = new BigDecimal(val);
        BigDecimal decimal1 = new BigDecimal(val1);
        System.out.println(decimal.subtract(decimal1).setScale(2, RoundingMode.HALF_UP));
        System.out.println(decimal.add(decimal1).setScale(2, RoundingMode.HALF_UP));
        System.out.println(decimal.divide(decimal1,2,RoundingMode.HALF_UP));
        System.out.println(decimal.multiply(decimal1).setScale(2,RoundingMode.HALF_UP));
    }
    public static void main(String[] args) {
//        mathMethod();
//        normalOperationMethod();
        BigDecimalMethod();

    }
}
