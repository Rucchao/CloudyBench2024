package com.cloudybench.sales;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.StringJoiner;

public record OrderLine
        (int OL_ID,
         int OL_O_ID,
         int OL_P_ID,
         int OL_QUANTITY,
         double OL_MOUNT,
         Date OL_UPDATED_DATE
        )
{
    @Override
    public String toString() {
        StringJoiner joiner = new StringJoiner(",");
        joiner.add(Integer.toString(OL_ID))
                .add(Integer.toString(OL_O_ID))
                .add(Integer.toString(OL_P_ID))
                .add(Integer.toString(OL_QUANTITY))
                .add(Double.toString(OL_MOUNT))
                .add(convertDateToString(OL_UPDATED_DATE));
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

