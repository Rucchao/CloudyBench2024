package com.cloudybench.sales;

/**
 * @time 2024-1-11
 * @version 1.0.0
 * @file Customer.java
 * @description
 *   for customer table
 **/
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.StringJoiner;

public record Customer
        (int C_ID,
         String C_NAME,
         String C_GENDER,
         int C_AGE,
         String C_PROVINCE,
         String C_CITY,
         double C_CREDIT,
         Date C_CREATED_DATE,
         Date C_UPDATED_DATE
        )
{
    @Override
    public String toString() {
        StringJoiner joiner = new StringJoiner(",");
        joiner.add(Integer.toString(C_ID))
                .add(C_NAME)
                .add(C_GENDER)
                .add(Integer.toString(C_AGE))
                .add(C_PROVINCE)
                .add(C_CITY)
                .add(Double.toString(C_CREDIT))
                .add(convertDateToString(C_CREATED_DATE))
                .add(convertDateToString(C_UPDATED_DATE));
        return joiner.toString();
    }

    public String convertDateToString(Date date)
    {
        // "yyyy-MM-dd"
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        String dateToString = df.format(date);
        return (dateToString);
    }
}

