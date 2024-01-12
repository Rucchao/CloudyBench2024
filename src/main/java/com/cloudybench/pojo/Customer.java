package com.cloudybench.pojo;
/**
 * Copyright (C) 2022-2023 China Software Testing Center(CSTC)
 * @time 2022-10-12
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
        (int custID,
         int companyID,
         String gender,
         String name,
         int age,
         String phone,
         String province,
         String city,
         int cityid,
         double loanBalance,
         long SavingCredit,
         long CheckingCredit,
         long LoanCredit,
         int isblocked,
         Date createdDate,
         Date LastUpdateDate
        )
{
    @Override
    public String toString() {
        StringJoiner joiner = new StringJoiner(",");
        joiner.add(Integer.toString(custID))
                .add(Integer.toString(companyID))
                .add(gender)
                .add(name)
                .add(Integer.toString(age))
                .add(phone)
                .add(province)
                .add(city)
                .add(Double.toString(loanBalance))
                .add(Long.toString(SavingCredit))
                .add(Long.toString(CheckingCredit))
                .add(Long.toString(LoanCredit))
                .add(String.valueOf(isblocked))
                .add(convertDateToString(createdDate))
                .add(convertDateToString(LastUpdateDate));
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

