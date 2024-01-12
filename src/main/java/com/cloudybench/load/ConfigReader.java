package com.cloudybench.load;
/**
 *
 * @time 2023-03-04
 * @version 1.0.0
 * @file ConfigReader.java
 * @description
 *
 **/

import com.moandjiezana.toml.Toml;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class ConfigReader {
    static final ClassLoader loader = ConfigReader.class.getClassLoader();
    public long sales_customer_number;
    public int sales_customer_age_lower;
    public int sales_customer_age_upper;
    public String sales_customer_datapath;
    public long sales_order_number;
    public long sales_product_number;
    public String sales_order_datapath;
    public String sales_orderline_datapath;
    public int startYear;
    public int midPoint;
    public int endYear;
    public Date startDate;
    public Date midPointDate;
    public Date endDate;
    public Date loanDate;


    public ConfigReader(String scale_factor){
        load("parameters.toml", scale_factor);
    }

    private void load(String fileName, String scale_factor) {
        try {
            BufferedReader Br = new BufferedReader(
                    new InputStreamReader(loader.getResourceAsStream(fileName)));
            Toml toml = new Toml().read(Br);
            // general
            startYear = toml.getLong(scale_factor + ".startYear").intValue();
            midPoint = toml.getLong(scale_factor + ".midPoint").intValue();
            endYear = toml.getLong(scale_factor + ".endYear").intValue();
            String startDate_str = toml.getString(scale_factor + ".startDate");
            String midDate_str = toml.getString(scale_factor + ".midPointDate");
            String endDate_str = toml.getString(scale_factor + ".endDate");
            startDate = new SimpleDateFormat("yyyy-MM-dd").parse(startDate_str);
            midPointDate = new SimpleDateFormat("yyyy-MM-dd").parse(midDate_str);
            endDate = new SimpleDateFormat("yyyy-MM-dd").parse(endDate_str);

            // sales customer
            sales_customer_number = toml.getLong(scale_factor + ".sales_customer_number");
            sales_customer_datapath = toml.getString(scale_factor + ".sales_customer_datapath");
            sales_customer_age_lower = toml.getLong(scale_factor + ".customer_age_lower").intValue();
            sales_customer_age_upper = toml.getLong(scale_factor + ".customer_age_upper").intValue();
            // sales order
            sales_order_number = toml.getLong(scale_factor + ".sales_order_number");
            sales_order_datapath = toml.getString(scale_factor + ".sales_order_datapath");
            // sales orderline
            sales_orderline_datapath = toml.getString(scale_factor + ".sales_orderline_datapath");
            // product
            sales_product_number = toml.getLong(scale_factor + ".sales_product_number");
        }
        catch (ParseException e){
            e.printStackTrace();
        }
    }
}
