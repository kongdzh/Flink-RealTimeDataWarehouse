package com.yankee.gmall.realtime.utils;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public class DateTimeUtil {
    public final static DateTimeFormatter formator = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static String toYmdHms(Date date) {
        LocalDateTime localDateTime = LocalDateTime.ofInstant(date.toInstant(), ZoneId.systemDefault());
        return formator.format(localDateTime);
    }

    public static Long toTs(String YmdHms) {
        LocalDateTime localDateTime = LocalDateTime.parse(YmdHms, formator);
        return localDateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli();
    }
}
