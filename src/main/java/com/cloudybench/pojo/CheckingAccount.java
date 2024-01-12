package com.cloudybench.pojo;
/**
 *
 * @time 2022-10-12
 * @author Chao Zhang
 * @version 1.0.0
 * @file CheckingAccount.java
 * @description
 *  for checkingaccount table
 **/
import java.util.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.StringJoiner;

/**
 * Created by chao zhang on 2022.10.12.
 */

public record CheckingAccount
        (int accountID,
         int userID,
         double balance,
         int isBlocked,
         Date timestamp
        )
{
    @Override
    public String toString() {
        StringJoiner joiner = new StringJoiner(",");
        joiner.add(Integer.toString(accountID))
                .add(Integer.toString(userID))
                .add(Double.toString(balance))
                .add(Integer.toString(isBlocked))
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

