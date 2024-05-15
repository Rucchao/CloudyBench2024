package com.cloudybench.util;

import com.cloudybench.load.DateUtility;
import okhttp3.*;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class NeonMetric {
    OkHttpClient client = new OkHttpClient();

    public NeonMetric() throws ParseException {
    }

    // code request code here
    String doGetRequest(String url) throws IOException {
        Request request = new Request.Builder()
                .url(url)
                .addHeader("accept", "application/json")
                .addHeader("content-type","application/json")
                .addHeader("authorization", "Bearer 5foyjvxag8e8pkrk44ka4o5upgoi4p3h446k8c2y5jddwy4reab1h4dikw6yd0qd")
                .build();

        Response response = client.newCall(request).execute();
        return response.body().string();
    }

    // post request code here

    public static final MediaType JSON
            = MediaType.parse("application/json");
    //; charset=utf-8
    // metric post data
    String dateformat(Date date)
    {
        // UTC time
        Date minus_hour = DateUtility.MinusHours(date,8);

        String dateString=DateUtility.convertDateToString(minus_hour);

        String dateFormat = dateString.split(" ")[0]+'T'+dateString.split(" ")[1]+'Z';

        return dateFormat;
    }

    public String metricJson(String start) throws ParseException {

        Date from = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(start);

        // compute a datatime after 10 mins
        // 10-minute range
        Date to = DateUtility.AddMinutes(from,10);

        String from_str=dateformat(from);

        String to_str=dateformat(to);

        return "{\"from\": \"" + from_str + "\",\"to\": \"" + to_str + "\",\"grouping\":\"min\",\"metrics\":[\"cpu_consumed_cores\",\"cpu_provisioned_cores\"]}";
    }

    public double doPostRequest(String url, String json) throws IOException {
        RequestBody body = RequestBody.create(JSON, json);
        //System.out.println(json);
        Request request = new Request.Builder()
                .url(url)
                .addHeader("accept", "application/json")
                .addHeader("content-type","application/json")
                .addHeader("authorization", "Bearer 5foyjvxag8e8pkrk44ka4o5upgoi4p3h446k8c2y5jddwy4reab1h4dikw6yd0qd")
                .post(body)
                .build();

        Response response  = client.newCall(request).execute();

        // decode the json and get the consumed resources
        JSONObject obj = new JSONObject(response.body().string());
        JSONArray data = obj.getJSONArray("data");
        JSONArray cpus=null;
        double total_used_cpus=0;

        for (int i = 0; i < data.length(); i++) {
            JSONObject metric = data.getJSONObject(i);
            if (metric.get("metric").equals("cpu_provisioned_cores")) {
                cpus = metric.getJSONArray("values");
                for (int j = 0; j < cpus.length(); j++) {
                    if(!JSONObject.NULL.equals(cpus.get(j))){
                        double used_cpus =(double) ((int) cpus.get(j));
                        total_used_cpus +=used_cpus;
                    }
                }
            }
        }

        return total_used_cpus;
    }


    // input
    //String json = metricJson("2024-05-13 19:48:36");


    // output

}
