package com.cloudybench.pojo;
/**
 *
 * @time 2022-10-12
 * @version 1.0.0
 * @file LoanTrans.java
 * @description
 *  For loanTrans table
 **/
import java.util.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.StringJoiner;


public record LoanTrans
        (int id,
         int applicantID,
         int appID,
         double amount,
         String status,
         Date timestamp,
         int duration,
         Date contract_timestamp,
         int delinquency
        )
{
    @Override
    public String toString() {
        StringJoiner joiner = new StringJoiner(",");
        String accept_timestamp="";
        if(contract_timestamp!=null)
            accept_timestamp=convertDateToString(contract_timestamp);
        joiner.add(Integer.toString(id))
                .add(Integer.toString(applicantID))
                .add(Integer.toString(appID))
                .add(Double.toString(amount))
                .add(status)
                .add(convertDateToString(timestamp))
                .add(Integer.toString(duration))
                .add(accept_timestamp)
                .add(Integer.toString(delinquency));
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

