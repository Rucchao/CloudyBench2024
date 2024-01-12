package com.cloudybench.sales;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.StringJoiner;

public record Order
        (int O_ID,
         int O_C_ID,
         Date O_DATE,
         String O_STATUS,
         double O_TOTALMOUNT,
         Date O_UPDATED_DATE
        )
{
    @Override
    public String toString() {
        StringJoiner joiner = new StringJoiner(",");
        joiner.add(Integer.toString(O_ID))
                .add(Integer.toString(O_C_ID))
                .add(convertDateToString(O_DATE))
                .add(O_STATUS)
                .add(Double.toString(O_TOTALMOUNT))
                .add(convertDateToString(O_UPDATED_DATE));
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

