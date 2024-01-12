package com.cloudybench.pojo;
/**
 *
 * @time 2022-10-12
 * @version 1.0.0
 * @file Company.java
 * @description
 *  for company table
 **/
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.StringJoiner;


public record Company
        (int companyID,
         String name,
         String category,
         int staffSize,
         double loanBalance,
         String phone,
         String province,
         String city,
         int cityId,
         long SavingCredit,
         long CheckingCredit,
         long LoanCredit,
         int isblocked,
         Date createdDate,
         Date LastUpdateTime
        )
{
    @Override
    public String toString() {
        StringJoiner joiner = new StringJoiner(",");
        joiner.add(Integer.toString(companyID))
                .add(name)
                .add(category)
                .add(Integer.toString(staffSize))
                .add(Double.toString(loanBalance))
                .add(phone)
                .add(province)
                .add(city)
                .add(Long.toString(SavingCredit))
                .add(Long.toString(CheckingCredit))
                .add(Long.toString(LoanCredit))
                .add(Integer.toString(isblocked))
                .add(convertDateToString(createdDate))
                .add(convertDateToString(LastUpdateTime));

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

