import com.cloudybench.util.NeonAPI;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.MediaType;
import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;

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
                            .addHeader("authorization", "Bearer xxx")
                            .post(body)
                            .build();
                    Response response = client.newCall(request).execute();
                    return response.body().string();
                }

        public static void main(String[] args) throws SQLException, IOException, ParseException {
            NeonAPI api= new NeonAPI();

    }
}
