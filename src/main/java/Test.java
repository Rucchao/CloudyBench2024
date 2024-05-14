import com.cloudybench.util.NeonMetric;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.MediaType;
import java.io.IOException;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.cloudybench.load.DateUtility;
import org.json.JSONArray;
import org.json.JSONObject;

public class Test {
                OkHttpClient client = new OkHttpClient();
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
                        = MediaType.parse("application/json; charset=utf-8");
                // metric post data
                String metricJson(String from, String to) {
                    return "{\"from\": \"" + from + "\" ,\"to\": \"" + to + "\" ,\"grouping\":\"min\",\"metrics\":[\"cpu_consumed_cores\",\"cpu_provisioned_cores\"]}";
                }

                String doPostRequest(String url, String json) throws IOException {
                    RequestBody body = RequestBody.create(JSON, json);
                    System.out.println(json);
                    Request request = new Request.Builder()
                            .url(url)
                            .addHeader("accept", "application/json")
                            .addHeader("content-type","application/json; charset=utf-8")
                            .addHeader("authorization", "Bearer 5foyjvxag8e8pkrk44ka4o5upgoi4p3h446k8c2y5jddwy4reab1h4dikw6yd0qd")
                            .post(body)
                            .build();
                    Response response = client.newCall(request).execute();
                    return response.body().string();
                }

        public static void main(String[] args) throws SQLException, IOException, ParseException {
            //System.out.println("04zt007bp26pvfvdrwvp6mjkjl7s03wryxx8t6psv01qnhtzu2w31puove2mxv8o");
            //Test example = new Test();
            //String json = example.metricJson("2024-05-13T05:40:53.937Z", "2024-05-13T05:50:53.937Z");

            // Step 2: obtain the resource consumption
            //String postResponse = example.doPostRequest("https://console.neon.tech/api/v2/projects/proud-bonus-37909019/endpoints/ep-muddy-dew-a1nrws5d/stats", json);

            // Step 3: decode the json results
            //String json= "{\"data\":[{\"metric\":\"cpu_consumed_cores\",\"values\":[null,null,null,null,0.39199999999999996,0.5110000000000001,0.9011666666666666,0.01783333333333332,0.9494999999999998,0.09916666666666667],\"labels\":{}},{\"metric\":\"cpu_provisioned_cores\",\"values\":[null,null,null,null,4,4,4,4,4,4],\"labels\":{}}],\"timestamps\":[1715578853,1715578913,1715578973,1715579033,1715579093,1715579153,1715579213,1715579273,1715579333,1715579393]}";
            //System.out.println(postResponse);
            //System.out.println(json);

//            JSONObject obj = new JSONObject(json);
//            JSONArray data = obj.getJSONArray("data");
//            JSONArray cpus=null;
//            double total_used_cpus=0;

//            for (int i = 0; i < data.length(); i++) {
//                JSONObject metric = data.getJSONObject(i);
//                if (metric.get("metric").equals("cpu_provisioned_cores")) {
//                    cpus = metric.getJSONArray("values");
//                    for (int j = 0; j < cpus.length(); j++) {
//                        if(!JSONObject.NULL.equals(cpus.get(j))){
//                            double used_cpus =(double) ((int) cpus.get(j));
//                            total_used_cpus +=used_cpus;
//                        }
//                    }
//                }
//            }
//            System.out.println(total_used_cpus);

//            // compute a datatime after 10 mins
//            Date date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse("2024-05-13 19:48:36");
//            System.out.println(date.toString());
//
//            // UTC time
//            Date minus_hour = DateUtility.MinusHours(date,8);
//
//            // 10-minute range
//            Date add_minutes = DateUtility.AddMinutes(minus_hour,10);
//
//            String dateString=DateUtility.convertDateToString(add_minutes);
//
//            System.out.println(dateString);
//
//            String dateFormat = dateString.split(" ")[0]+'T'+dateString.split(" ")[1]+'Z';
//
//            System.out.println(dateFormat);

            // Step 3: compute the E1-score
                // 3.1 run the elastic workload
                // 3.2 collect the execution time
                // 3.3 compute the E1-Score

            NeonMetric neon = new NeonMetric();

            String json=neon.metricJson("2024-05-13 19:48:36");
            String url="https://console.neon.tech/api/v2/projects/proud-bonus-37909019/endpoints/ep-muddy-dew-a1nrws5d/stats";

            double cpus = neon.doPostRequest(url,json);
            System.out.println("your consumed cpus is "+cpus);

    }
}
