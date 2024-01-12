package com.cloudybench.pojo;
/**
 *
 * @time 2022-10-12
 * @version 1.0.0
 * @file Transfer.java
 * @description
 *      For transfer table
 **/
import java.util.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.StringJoiner;


public record Transfer
        (long id,
         int sourceID,
         int targetID,
         double amount,
         String type,
         Date timestamp,
         Date fresh_ts
        )
{
    public String toString() {
        StringJoiner joiner = new StringJoiner(",");
        joiner.add(Long.toString(id))
                .add(Integer.toString(sourceID))
                .add(Integer.toString(targetID))
                .add(Double.toString(amount))
                .add(type)
                .add(convertDateToString(timestamp))
                .add("");
        return joiner.toString();
    }

    public String convertDateToString(java.util.Date date)
    {
        // "yyyy-MM-dd HH:mm:ss.SSS"
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        String dateToString = df.format(date);
        return (dateToString);
    }
}