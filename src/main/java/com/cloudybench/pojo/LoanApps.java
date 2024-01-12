package com.cloudybench.pojo;
/**
 *
 * @time 2022-10-12
 * @version 1.0.0
 * @file LoanApps.java
 * @description
 *  For loanApps table
 **/
import java.util.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.StringJoiner;


public record LoanApps
        (int id,
         int applicantID,
         double amount,
         int duration,
         String status,
         // boolean Inprocess,
         Date app_timestamp
         //Date last_update_timestamp
        )
{
    @Override
    public String toString() {
        StringJoiner joiner = new StringJoiner(",");
        joiner.add(Integer.toString(id))
                .add(Integer.toString(applicantID))
                .add(Double.toString(amount))
                .add(Integer.toString(duration))
                .add(status)
                .add(convertDateToString(app_timestamp));
                //.add(convertDateToString(last_update_timestamp));
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