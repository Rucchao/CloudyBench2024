package com.cloudybench.pojo;
/**
 *
 * @time 2022-10-12
 * @version 1.0.0
 * @file SavingAccount.java
 * @description
 *  For savingAccount table
 **/
import java.util.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.StringJoiner;



public record SavingAccount
        (int accountID,
         int userID,
         double balance,
         int isBlocked,
         Date timestamp,
         Date fresh_ts
        )
{
    @Override
    public String toString() {
        StringJoiner joiner = new StringJoiner(",");
        joiner.add(Integer.toString(accountID))
                .add(Integer.toString(userID))
                .add(Double.toString(balance))
                .add(Integer.toString(isBlocked))
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

