package com.code.date;

import java.sql.Timestamp;
import java.time.*;
import java.time.format.DateTimeFormatter;

/**
 * @author kuiqwang
 * @emai wqkenqingto@163.com
 * @time 2023/2/20
 * @desc 日期类处理，主要是java8为主
 */
public class DateOperateDemo {


    public static void dateMethod() {
        //获的localDate
        LocalDate localDate = LocalDate.now();
        System.out.println(localDate);
        LocalTime localTime = LocalTime.now();
        System.out.println(localTime);
        LocalDateTime localDateTime = LocalDateTime.now();
        System.out.println(localDateTime);
        LocalTime time = LocalTime.of(9, 30, 0);
        LocalDateTime datetime = LocalDateTime.of(2023, 2, 20, 9, 30, 0);
        System.out.println(datetime);
        ZoneId zoneId = ZoneId.of("America/New_York");
//        ZoneId zoneIdNew = ZoneId.of("Asia/Shanghai");
        ZonedDateTime zonedDateTime = ZonedDateTime.of(2023, 2, 20, 9, 30, 0, 0, zoneId);
        System.out.println(zonedDateTime);
        Duration duration = Duration.ofHours(2);
        System.out.println(duration.toHours());
        Period period = Period.of(1, 6, 0);
        System.out.println(period.toString());
        System.out.println(period.getYears());
        //time convert
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("hh:mm:ss a");
        System.out.println(time.format(formatter));
        String timeString = "09:30:00 上午";
        LocalTime time2 = LocalTime.parse(timeString, formatter);
        System.out.println(time2);
        System.out.println(Timestamp.valueOf(localDateTime).getTime());
        Instant instant = Instant.now();
        System.out.println(instant.toEpochMilli());
        ZoneId zone = ZoneId.systemDefault();
        System.out.println(LocalDateTime.ofInstant(instant, zone));
        System.out.println(localDateTime.atZone(zone).toInstant().toEpochMilli());

    }

    public static void main(String[] args) {
        dateMethod();
    }
}
