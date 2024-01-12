package com.cloudybench.pojo;
/**
 *
 * @time 2022-10-12
 * @author Chao Zhang
 * @version 1.0.0
 * @file Checking.java
 * @description
 *  for checking table
 **/
import java.util.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.StringJoiner;

/**
 * Created by chao zhang on 2022.10.12.
 */

public record Checking
        (int id,
         int sourceID,
         int targetID,
         double amount,
         String type,
         Date timestamp
        )
{
    public String toString() {
        StringJoiner joiner = new StringJoiner(",");
        joiner.add(Integer.toString(id))
                .add(Integer.toString(sourceID))
                .add(Integer.toString(targetID))
                .add(Double.toString(amount))
                .add(type)
                .add(convertDateToString(timestamp));
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