package com.cloudybench.load;

/**
 * @time 2024-1-11
 * @version 1.0.0
 * @file DataGenerator_Sales.java
 * @description
 *
 **/

import com.cloudybench.sales.*;
import com.cloudybench.util.RandomGenerator;

import java.io.*;
import java.util.*;

public class DataGenerator_Sales {
    private int sf = 1;
    String config_key="cloudybench";

    public DataGenerator_Sales(int sf){
        this.sf = sf;
    }

    public void dataGenerator() {
        System.out.println("This is a data generator of CloudyBench, Version 0.1");
        System.out.println("----------------");
        System.out.println("----------------");
        System.out.println("----------------");
        System.out.println("Data is generating...");
        System.out.println("----------------");
        System.out.println("----------------");
        System.out.println("----------------");
        long millisStart = System.currentTimeMillis();

        DataSource DS = new DataSource();
        RandomGenerator RG = new RandomGenerator();
        // input the scale factor, e.g. 1x or 10x
        ConfigReader CR = new ConfigReader(config_key);
        FileWriter cust_fileWriter = null;
        BufferedWriter cust_bufferedWriter = null;
        String cust_writePath = CR.sales_customer_datapath;

        FileWriter order_fileWriter = null;
        BufferedWriter order_bufferedWriter = null;
        String order_writePath = CR.sales_order_datapath;

        FileWriter orderline_fileWriter = null;
        BufferedWriter orderline_bufferedWriter = null;
        String orderline_writePath = CR.sales_orderline_datapath;

        String DataPath = "Data_" + "SF" + sf  + "/";
        String NEW_LINE_SEPARATOR = "\n";
        Long customerNumber = CR.sales_customer_number;
        int customer_size = customerNumber.intValue() * 10 * sf;

        Long orderNumber=CR.sales_order_number;
        Long productNumber= CR.sales_product_number;
        int order_size=orderNumber.intValue() * 10 * sf;
        int product_size=productNumber.intValue() * sf;

        File directory = new File(DataPath);
        if (!directory.exists())
            directory.mkdirs();
        try {
            cust_fileWriter = new FileWriter(DataPath + cust_writePath, false);
            cust_bufferedWriter = new BufferedWriter(cust_fileWriter);

            order_fileWriter = new FileWriter(DataPath + order_writePath, false);
            order_bufferedWriter = new BufferedWriter(order_fileWriter);

            orderline_fileWriter = new FileWriter(DataPath + orderline_writePath, false);
            orderline_bufferedWriter = new BufferedWriter(orderline_fileWriter);

            // generate the Customers
            for (int i = 1; i <= customer_size; i++) {
                // generate the gender
                String gender = RG.getRandomString(DS.gender);
                StringBuilder Namebuilder = new StringBuilder();
                Namebuilder.append(RG.getRandomItem(DS.LastName_list)).append(" ");
                // generate first name with gender
                if (gender.equals("female"))
                    Namebuilder.append(RG.getRandomItem(DS.FirstName_female_list));
                else Namebuilder.append(RG.getRandomItem(DS.FirstName_male_list));
                // generate the age
                int age = RG.getRandomint(CR.sales_customer_age_lower, CR.sales_customer_age_upper);
                // generate the province and citylist
                String province = RG.getRandomProvince();
                List<String> citylist = DS.Province_Cities_Map.get(province);
                // generate the city
                String city = RG.getRandomItem(citylist);
                // generate the timestamp
                Date date = RG.getRandomTimestamp(CR.startDate, CR.midPointDate);
                // generate the blocked label
                int blocked=0;

                // generate a new customer
                Customer cust = new Customer(i, Namebuilder.toString(), gender, age, province, city, 0, date, date);
                cust_bufferedWriter.write(cust.toString());
                cust_bufferedWriter.write(NEW_LINE_SEPARATOR);

            }
            cust_bufferedWriter.flush();
            cust_bufferedWriter.close();

            // generate an Order and ten Orderlines
            for (int j = 1; j <= order_size; j++) {
                // generate custid
                int custid=RG.getRandomint(1, customer_size);

                // generate status
                String status=RG.getRandomOrderStatus();

                // generate date
                Date orderDate = RG.getRandomTimestamp(CR.midPointDate, CR.endDate);

                // caculate the total amount
                double total_amount=0;
                for (int i = 1; i <= 10; i++) {
                    int OL_ID = (j-1) * 10 + i;
                    int OL_P_ID = RG.getRandomint(1,product_size);
                    int OL_QUANTITY= RG.getRandomint(1,5);
                    double OL_AMOUNT=RG.getRandomDouble(10.0);
                    total_amount += OL_AMOUNT * OL_QUANTITY;

                    OrderLine ol = new OrderLine(OL_ID, j, OL_P_ID, OL_QUANTITY, OL_AMOUNT, orderDate);
                    orderline_bufferedWriter.write(ol.toString());
                    orderline_bufferedWriter.write(NEW_LINE_SEPARATOR);
                }

                Order o = new Order(j, custid, orderDate, status,RG.round(total_amount,2),orderDate);
                order_bufferedWriter.write(o.toString());
                order_bufferedWriter.write(NEW_LINE_SEPARATOR);
            }

            order_bufferedWriter.flush();
            order_bufferedWriter.close();

            orderline_bufferedWriter.flush();
            orderline_bufferedWriter.close();

        } catch (IOException e) {
            e.printStackTrace();
        }

        long millisEnd = System.currentTimeMillis();
        System.out.println("Data is ready under the Data folder!");
        System.out.println("----------------");
        System.out.println("----------------");
        System.out.println("----------------");
        System.out.println("Data generation took "+(millisEnd - millisStart) + " ms");
    }
}
