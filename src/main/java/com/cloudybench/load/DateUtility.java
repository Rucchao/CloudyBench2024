package com.cloudybench.load;
/**
 *
 * @time 2023-03-04
 * @version 1.0.0
 * @file DateUtility.java
 * @description
 *  Define date type and normalization
 **/
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.Month;
import java.time.ZoneId;
import java.util.Date;
import java.util.concurrent.TimeUnit;

public class DateUtility {
    public static ZoneId UTC = ZoneId.of("UTC");

    public static long toEpochMilli(LocalDate ld) {
        return ld.atStartOfDay(UTC).toInstant().toEpochMilli();
    }

    public static LocalDate utcDateOfEpochMilli(long epochMilli) {
        return Instant.ofEpochMilli(epochMilli).atZone(UTC).toLocalDate();
    }

    public static int getNumberOfMonths(long epochMilli, int startMonth, int startYear) {
        LocalDate date = utcDateOfEpochMilli(epochMilli);
        int month = date.getMonthValue();
        int year = date.getYear();
        return (year - startYear) * 12 + month - (startMonth - 1);
    }

    public static int getYear(long epochMilli) {
        LocalDate date = utcDateOfEpochMilli(epochMilli);
        return date.getYear();
    }

    public static Month getMonth(long epochMilli) {
        LocalDate date = utcDateOfEpochMilli(epochMilli);
        return date.getMonth();
    }

    public static int getDay(long epochMilli) {
        LocalDate date = utcDateOfEpochMilli(epochMilli);
        return date.getDayOfMonth();
    }

    public static Date OneDayAfter(Date originalDate) {
        long aDay = TimeUnit.DAYS.toMillis(1);
        long now = originalDate.getTime();
        Date Dateplus1day = new Date(now + aDay);
        return Dateplus1day;
    }

    public static Date AddMinutes(Date originalDate, int add) {
        long delta = TimeUnit.MINUTES.toMillis(add);
        long now = originalDate.getTime();
        Date newdate = new Date(now + delta);
        return newdate;
    }

    public static Date MinusHours(Date originalDate, int add) {
        long delta = TimeUnit.HOURS.toMillis(add);
        long now = originalDate.getTime();
        Date newdate = new Date(now - delta);
        return newdate;
    }

    public static String convertDateToString(Date date)
    {
        // "yyyy-MM-dd HH:mm:ss.SSS"
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        String dateToString = df.format(date);
        return (dateToString);
    }

    public static Date convertStringToDate(String dateString) throws ParseException {
        // "yyyy-MM-dd HH:mm:ss.SSS"
        Date date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").parse(dateString);
        return date;
    }
}
