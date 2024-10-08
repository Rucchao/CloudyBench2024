package com.cloudybench.util;

import com.cloudybench.CloudyBench;
import com.cloudybench.load.DateUtility;
import okhttp3.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Level;

public class NeonAPI {
    OkHttpClient client = new OkHttpClient();
    public static Logger neologger = LogManager.getLogger(NeonAPI.class);

    //.setLevel(Level.FINE);

    public NeonAPI() throws ParseException {
    }

    // code request code here
    String doGetRequest(String url) throws IOException {
        Request request = new Request.Builder()
                .url(url)
                .addHeader("accept", "application/json")
                .addHeader("content-type","application/json")
                .addHeader("authorization", "Bearer xxx")
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
        Date minus_hour = DateUtility.MinusHours(date,0);

        String dateString=DateUtility.convertDateToString(minus_hour);

        String dateFormat = dateString.split(" ")[0]+'T'+dateString.split(" ")[1]+'Z';

        return dateFormat;
    }

    public String metricJson(String start, int range) throws ParseException {

        Date from = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(start);
        System.out.println(start);
        System.out.println(from);

        // compute a datatime after 10 mins
        // 10-minute range
        Date to = DateUtility.AddMinutes(from,range);

        String from_str=dateformat(from);

        String to_str=dateformat(to);

        return "{\"from\": \"" + from_str + "\",\"to\": \"" + to_str + "\",\"grouping\":\"min\",\"metrics\":[\"cpu_consumed_cores\",\"cpu_provisioned_cores\"]}";
    }

    public double doPostRequest(String url, String json, String key) throws IOException {
        RequestBody body = RequestBody.create(JSON, json);
        //System.out.println(json);
        Request request = new Request.Builder()
                .url(url)
                .addHeader("accept", "application/json")
                .addHeader("content-type","application/json")
                .addHeader("authorization", "Bearer " + key)
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
                        double used_cpus = Double.parseDouble(String.valueOf(cpus.get(j)));
                        total_used_cpus +=used_cpus;
                    }
                }
            }
        }

        return total_used_cpus;
    }
    
    public String getEndpoint(String url) throws IOException {
        OkHttpClient client = new OkHttpClient();

        Request request = new Request.Builder()
                .url(url)
                .get()
                .addHeader("accept", "application/json")
                .addHeader("authorization", "Bearer xxx")
                .build();

        Response response = client.newCall(request).execute();

        return response.body().string();
    }

    // type 0: suspend, type 1: start, type 2: restart
    public void Endpoint(String url, int type, String key) throws IOException {
        OkHttpClient client = new OkHttpClient();
        RequestBody body = RequestBody.create(JSON, "{}");
        switch (type){
            case 0:
                url=url+"/suspend";
                break;
            case 1:
                url=url+"/start";
                break;
            case 2:
                url=url+"/restart";
                break;
        }

        Request request = new Request.Builder()
                .url(url)
                .post(body)
                .addHeader("accept", "application/json")
                .addHeader("authorization", "Bearer " + key)
                .build();

        Response response = client.newCall(request).execute();
        neologger.info(response.body().string());
        response.close();
        //System.out.println(response.body().string());

    }
}
