package com.cloudybench;

import com.cloudybench.load.DataGenerator_Sales;
import com.cloudybench.load.ExecSQL;
import com.cloudybench.stats.Result;
import com.cloudybench.util.NeonAPI;
import com.cloudybench.workload.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class CloudyBench {
    public static Logger logger = LogManager.getLogger(CloudyBench.class);
    int taskType = 0;
    Result res = new Result();
    boolean verbose = true;
    int tenant_num=0;
    int TP_tenant_num=0;
    int AP_tenant_num=0;
    Sqlstmts sqls = null;
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public Result getRes(){
        return res;
    }

    void setSqls(Sqlstmts sqlloader) {
        this.sqls = sqlloader;
    }

    Sqlstmts getSqls(){
        return sqls;
    }

    // Measure lag time between primary and replica
    public void runLagTime(){
        logger.info("Begin TP Workload");
        taskType = 1;
        res.setStartTS(dateFormat.format(new Date()));
        String tpClient = ConfigLoader.prop.getProperty("tpclient");

        List<Client> tasks = new ArrayList<Client>();
        if(Integer.parseInt(tpClient) > 0){
            Client job = Client.initTask(ConfigLoader.prop,"CloudLagTime",taskType);
            job.setRet(res);
            job.setVerbose(verbose);
            job.setSqls(sqls);
            tasks.add(job);
        }
        else {
            logger.warn("There is no an available tp client");
            return;
        }
        ExecutorService es = Executors.newFixedThreadPool(tasks.size());
        List<Future> future = new ArrayList<Future>();
        for (final Client j : tasks) {
            future.add( es.submit(new Runnable() {
                        public void run() {
                            j.startTask();
                        }
                    })
            );
        }
        for(int flength=0;flength < future.size();flength++) {
            Future f = future.get(flength);
            if (f != null && !f.isCancelled() && !f.isDone()) {
                try {
                    f.get();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
            }
        }

        if (!es.isShutdown() || !es.isTerminated()) {
            es.shutdownNow();
        }
        res.setEndTs(dateFormat.format(new Date()));
        logger.info("TP Workload is done.");
    }

    // Measure read-write throughput between primary and replica
    public void runReplica(int taskType){
        logger.info("Begin TP Workload");
        res.setStartTS(dateFormat.format(new Date()));
        String tpClient = ConfigLoader.prop.getProperty("tpclient");

        List<Client> tasks = new ArrayList<Client>();
        if(Integer.parseInt(tpClient) > 0){
            Client job = Client.initTask(ConfigLoader.prop,"CloudReplica",taskType);
            job.setRet(res);
            job.setVerbose(verbose);
            job.setSqls(sqls);
            tasks.add(job);
        }
        else {
            logger.warn("There is no an available tp client");
            return;
        }
        ExecutorService es = Executors.newFixedThreadPool(tasks.size());
        List<Future> future = new ArrayList<Future>();
        for (final Client j : tasks) {
            future.add( es.submit(new Runnable() {
                        public void run() {
                            j.startTask();
                        }
                    })
            );
        }
        for(int flength=0;flength < future.size();flength++) {
            Future f = future.get(flength);
            if (f != null && !f.isCancelled() && !f.isDone()) {
                try {
                    f.get();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
            }
        }

        if (!es.isShutdown() || !es.isTerminated()) {
            es.shutdownNow();
        }
        res.setEndTs(dateFormat.format(new Date()));
        logger.info("TP Workload is done.");
    }

    // Measure the fail-over performance
    public void Failover(int taskType){
        logger.info("Begin TP Workload");
        res.setStartTS(dateFormat.format(new Date()));
        String tpClient = ConfigLoader.prop.getProperty("tpclient");

        List<Client> tasks = new ArrayList<Client>();
        if(Integer.parseInt(tpClient) > 0){
            Client job = Client.initTask(ConfigLoader.prop,"CloudFailover",taskType);
            job.setRet(res);
            job.setVerbose(verbose);
            job.setSqls(sqls);
            tasks.add(job);
        }
        else {
            logger.warn("There is no an available tp client");
            return;
        }
        ExecutorService es = Executors.newFixedThreadPool(tasks.size());
        List<Future> future = new ArrayList<Future>();
        for (final Client j : tasks) {
            future.add( es.submit(new Runnable() {
                        public void run() {
                            j.startTask();
                        }
                    })
            );
        }
        for(int flength=0;flength < future.size();flength++) {
            Future f = future.get(flength);
            if (f != null && !f.isCancelled() && !f.isDone()) {
                try {
                    f.get();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
            }
        }

        if (!es.isShutdown() || !es.isTerminated()) {
            es.shutdownNow();
        }
        res.setEndTs(dateFormat.format(new Date()));
        logger.info("TP Workload is done.");
    }

    // run TP type workload. Spouse nums of threads defined in conf file.
    public void runCloudTP(int tenant_num, int[] Conlist){
        logger.info("Begin Cloud TP Workload");
        taskType = 8;
        List<Client> tasks = new ArrayList<Client>();
        res.setStartTS(dateFormat.format(new Date()));
        res.setTenant_num(tenant_num);
        for (int i = 1; i <=tenant_num ; i++) {
            //String tpClient = ConfigLoader.prop.getProperty("tpclient_"+i);
            int client=Conlist[i-1];
            logger.info("Tenant"+ i + " has a concurrency of "+client);
            if(client > 0){
                Client job = Client.initTask(ConfigLoader.prop,"CloudTPClient",taskType, i, client, tenant_num);
                //Result res_tenant = new Result(i);
                job.setRet(res);
                job.setVerbose(verbose);
                job.setSqls(sqls);
                tasks.add(job);
            } else {
                logger.warn("There is no an available tp client");
            }
        }
        if(tasks.size()<1) {

            long currentTime = System.currentTimeMillis();
            long waitTime = 60 * 1000L;
            long endTime = currentTime + waitTime;

            logger.info("Wait for a minute");
            try {
                Thread.sleep(waitTime);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("A minute complete.");

            logger.info("====================Test Summary========================");
            logger.info("Test starts at " + dateFormat.format(currentTime));
            logger.info("Test ends at " + dateFormat.format(endTime));
            logger.info("Concurrency = 0");
            logger.info("====================Thank you!========================");

            return;
        }

        ExecutorService es = Executors.newFixedThreadPool(tasks.size());
        List<Future> future = new ArrayList<Future>();
        for (final Client j : tasks) {
            future.add( es.submit(new Runnable() {
                        public void run() {
                            j.startTask();
                        }
                    })
            );
        }
        for(int flength=0;flength < future.size();flength++) {
            Future f = future.get(flength);
            if (f != null && !f.isCancelled() && !f.isDone()) {
                try {
                    f.get();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
            }
        }

        if (!es.isShutdown() || !es.isTerminated()) {
            es.shutdownNow();
        }

        res.setEndTs(dateFormat.format(new Date()));

        logger.info("Cloud TP Workload is done.");
    }

    public void runElastic(ConfigLoader config) throws ParseException, IOException {
        String elastic_testTime=config.prop.getProperty("elastic_testTime");
        String first_con_str=config.prop.getProperty("first_con");
        String second_con_str=config.prop.getProperty("second_con");
        String third_con_str=config.prop.getProperty("third_con");
        int[][] Con=null;
        int total_test_time = Integer.parseInt(elastic_testTime);
        int first_con = Integer.parseInt(first_con_str);;
        int second_con = Integer.parseInt(second_con_str);
        int third_con = Integer.parseInt(third_con_str);

        // Workload Pattern Generation
        Con= new int[total_test_time][1];

        // the concurrency in the first minute
        Con[0][0]=first_con;
        // the concurrency in the second minute
        Con[1][0]=second_con;
        // the concurrency in the third minute
        Con[2][0]=third_con;

        Instant instant = Instant.now();
        System.out.println(instant);
        String[] str = Instant.now().toString().split("T");
        String startTime = str[0] + " " + str[1].substring(0, 8);

        logger.info("Elastic test starts at " + startTime);
        double total_tps=0;

        for (int i = 1; i <= total_test_time; i++) {
            logger.info("This is the "+i+"-th time slot...");
            runCloudTP(1, Con[i-1]);
            double tps = 0;
            if (!(Con[i - 1][0] == 0)) {
                tps = res.getTpsList()[0];
            }
            total_tps+=tps;
        }
        // need to compute the average TPS for three minutes
        System.out.println("====================Elasticity Summary========================");

        double avg_tps=total_tps/3;

        System.out.printf("The elastic average tps is  : %10.2f \n", avg_tps  * 1.0);

        String cdb = config.prop.getProperty("cdb","neon");

        if(cdb.equals("neon")){
            // caculate the resources
            NeonAPI neon = new NeonAPI();
            String json=neon.metricJson(startTime, 10);
            String url = config.prop.getProperty("metric_url","404");
            String key = config.prop.getProperty("authentication_key");
            double cpus = neon.doPostRequest(url,json, key);
            double rcu_c = Double.parseDouble(config.prop.getProperty("rcu_c","0"))/60;
            double rcu_m = Double.parseDouble(config.prop.getProperty("rcu_m","0"))/60;
            double rcu_io = Double.parseDouble(config.prop.getProperty("rcu_io","0"))/60;
            int cpu_mem_ratio=Integer.parseInt(config.prop.getProperty("cpu_mem_ratio","1"));
            int IOPS = Integer.parseInt(ConfigLoader.prop.getProperty("IOPS","1"))/60;
            double resource_cost=cpus * rcu_c+cpus * rcu_m * cpu_mem_ratio + rcu_io * IOPS;
            System.out.println("-----------E1-Score--------------------");
            double E1_Score= (avg_tps/resource_cost)  * 1.0;
            System.out.printf("The E1-Score is   : %10.5f \n", E1_Score);
            res.setElastic_tps(avg_tps);
            res.setE1_SCORE(E1_Score);
        }
    }

    public void runScaling(ConfigLoader config){
        System.out.println("====================The RW is running================================");
        runReplica(3);
        System.out.println("====================The RW and RO are running========================");
        runReplica(2);
        System.out.println("====================Scaling Summary==================================");

        int replica_num=Integer.parseInt(config.prop.getProperty("node_num","1"));

        System.out.printf("The RW average tps is  : %10.2f \n", res.getTps_rw()  * 1.0);

        System.out.printf("The RW-RO average tps is  : %10.2f \n", res.getTps_ro()  * 1.0);

        System.out.println("-----------E2-Score--------------------");

        double E2_Score = (res.getTps()/(res.getTps_rw()*replica_num))  * 1.0;

        res.setE2_SCORE(E2_Score);

        System.out.printf("The E2-Score is   : %10.5f \n", E2_Score);
    }

    public void runFailover(){
        // RW-RO
        Failover(5);
        // RW
        Failover(4);

        System.out.println("====================Failover Summary========================");

        System.out.println("-----------F-Score--------------------");
        //System.out.println(res.getF_Score_RO());
        //System.out.println(res.getF_Score_RW());
        long F_Score = (res.getF_Score_RO()+res.getF_Score_RW())/2;
        res.setF_Score(F_Score);

        System.out.printf("The F-Score is  : %d \n", F_Score);

        System.out.println("-----------R-Score--------------------");
        System.out.println("====================Failover Summary========================");
//        System.out.println(res.getR_Score_RO());
//        System.out.println(res.getR_Score_RW());
        long R_Score = (res.getR_Score_RO()+res.getR_Score_RW())/2;
        res.setR_Score(R_Score);
        System.out.printf("The R-Score is  : %d \n", R_Score);
    }

    public void runTenancy(ConfigLoader config) throws ParseException, IOException {
        String elastic_testTime=config.prop.getProperty("elastic_testTime");
        int total_test_time = Integer.parseInt(elastic_testTime);

        // configure the multi-tenancy workload
        String first_con_1_str=config.prop.getProperty("first_con_1");
        String second_con_1_str=config.prop.getProperty("second_con_1");
        String third_con_1_str=config.prop.getProperty("third_con_1");

        String first_con_2_str=config.prop.getProperty("first_con_2");
        String second_con_2_str=config.prop.getProperty("second_con_2");
        String third_con_2_str=config.prop.getProperty("third_con_2");

        String first_con_3_str=config.prop.getProperty("first_con_3");
        String second_con_3_str=config.prop.getProperty("second_con_3");
        String third_con_3_str=config.prop.getProperty("third_con_3");

        int first_con_1 = Integer.parseInt(first_con_1_str);
        int second_con_1 = Integer.parseInt(second_con_1_str);
        int third_con_1 = Integer.parseInt(third_con_1_str);

        int first_con_2 = Integer.parseInt(first_con_2_str);
        int second_con_2 = Integer.parseInt(second_con_2_str);
        int third_con_2 = Integer.parseInt(third_con_2_str);

        int first_con_3 = Integer.parseInt(first_con_3_str);
        int second_con_3 = Integer.parseInt(second_con_3_str);
        int third_con_3 = Integer.parseInt(third_con_3_str);

        int[][] Con=null;
        // Workload Pattern Generation
        Con= new int[total_test_time][3];

        // the concurrency in the first minute
        Con[0][0]=first_con_1;
        Con[0][1]=first_con_2;
        Con[0][2]=first_con_3;

        // the concurrency in the second minute
        Con[1][0]=second_con_1;
        Con[1][1]=second_con_2;
        Con[1][2]=second_con_3;

        // the concurrency in the third minute
        Con[2][0]=third_con_1;
        Con[2][1]=third_con_2;
        Con[2][2]=third_con_3;

        Instant instant = Instant.now();
        System.out.println(instant);
        String[] str = Instant.now().toString().split("T");
        String startTime = str[0] + " " + str[1].substring(0, 8);

        logger.info("Multi-tenancy test starts at " + startTime);
        double total_tenant1_tps=0;
        double total_tenant2_tps=0;
        double total_tenant3_tps=0;

        for (int i = 1; i <= total_test_time; i++) {
            logger.info("This is the " + i + "-th time slot...");
            runCloudTP(3, Con[i - 1]);
            for (int j = 0; j < 3; j++) {
                if (Con[i - 1][j] != 0) {
                    total_tenant1_tps += res.getTpsList()[0];
                    total_tenant2_tps += res.getTpsList()[1];
                    total_tenant3_tps += res.getTpsList()[2];
                    break;
                }
            }
        }

        System.out.println("====================Multi-Tenancy Summary========================");

        double geomean_tps=Math.pow(total_tenant1_tps/3*total_tenant2_tps/3*total_tenant3_tps/3, 1/3.0);

        res.setTps_geometric(geomean_tps);

        System.out.printf("The multitenancy average tps is  : %10.5f \n",geomean_tps * 1.0);

        String cdb = config.prop.getProperty("cdb","neon");

        if(cdb.equals("neon")){
            // caculate the total resource cost
            NeonAPI neon = new NeonAPI();
            String json=neon.metricJson(startTime, 3);
            Double rcu_c = Double.parseDouble(ConfigLoader.prop.getProperty("rcu_c","1")) / 60;
            Double rcu_m = Double.parseDouble(ConfigLoader.prop.getProperty("rcu_m","1")) / 60;
            Double rcu_io = Double.parseDouble(ConfigLoader.prop.getProperty("rcu_io","1")) / 60;
            Double rcu_mbps = Double.parseDouble(ConfigLoader.prop.getProperty("rcu_gbps","1")) / 60;
            Double rcu_s = Double.parseDouble(ConfigLoader.prop.getProperty("rcu_s","1")) / 60;
            int cpu_num = Integer.parseInt(ConfigLoader.prop.getProperty("cpu_num","1"));
            int mem_num = Integer.parseInt(ConfigLoader.prop.getProperty("mem_num","1"));
            int IOPS = Integer.parseInt(ConfigLoader.prop.getProperty("IOPS","1"));
            int Network = Integer.parseInt(ConfigLoader.prop.getProperty("Network","1"));
            int node_num = Integer.parseInt(ConfigLoader.prop.getProperty("node_num","1"));
            int store = Integer.parseInt(ConfigLoader.prop.getProperty("store","1"));
            String key = config.prop.getProperty("authentication_key");

            // tenant 1
            String url_1 = config.prop.getProperty("metric_url_1","404");
            double cpus_1 = neon.doPostRequest(url_1,json, key);

            // tenant 2
            String url_2 = config.prop.getProperty("metric_url_2","404");
            double cpus_2 = neon.doPostRequest(url_2,json, key);

            // tenant 3
            String url_3 = config.prop.getProperty("metric_url_3","404");
            double cpus_3 = neon.doPostRequest(url_3,json, key);

            double resource_cost=(rcu_c*cpu_num+rcu_m*mem_num+rcu_io*IOPS+rcu_mbps*Network+rcu_s*store)*node_num*3;
            resource_cost = resource_cost / total_test_time;
            System.out.println("-----------T-Score--------------------");
            System.out.printf("The average resource cost is   : %10.2f \n", resource_cost);
            double T_Score = geomean_tps/resource_cost* 1.0;
            res.setT_Score(T_Score);
            System.out.printf("The T-Score is   : %10.10f \n", T_Score);
        }
    }

    public static void main(String[] args) throws SQLException, ParseException, IOException {
        LoggerContext context = (LoggerContext) LogManager.getContext(false);
        File file = new File("conf/log4j2.properties");
        context.setConfigLocation(file.toURI());
        ConfigLoader config = new ConfigLoader();
        CloudyBench bench = new CloudyBench();
        int[][] Con=null;
        logger.info("Hi~ this is CloudyBench");
        int total_test_time = 0;
        CommandProcessor cmdProcessor = new CommandProcessor(args);
        HashMap<String,String> argsList = cmdProcessor.commandPaser(args);
        int type = 0;
        String cmd = null;
        if(!argsList.containsKey("t") || !argsList.containsKey("c")){
            logger.error("Missing required options -t or -c,please check");
            cmdProcessor.printHelp();
            System.exit(-1);
        }

        if(argsList.containsKey("c")){
            ConfigLoader.confFile = argsList.get("c");
            config.loadConfig();
            config.printConfig();
        }

        if(argsList.containsKey("s")){
            bench.verbose = false;
        }

        if(argsList.containsKey("m")){
            String num = argsList.get("m");
            bench.TP_tenant_num = Integer.parseInt(num);
        }

        if(argsList.containsKey("t")){
            cmd = argsList.get("t");
            if(cmd.equalsIgnoreCase("sql")){
                if(argsList.containsKey("f")){
                    ExecSQL execSQL = new ExecSQL(argsList.get("f"));
                    execSQL.execute();
                }
                else{
                    logger.error("Maybe missing sql file argument -f ,please try to use help to check usage.");
                }
            }
            else if(cmd.equalsIgnoreCase("gendata")){
                DataGenerator_Sales new_dg = new DataGenerator_Sales(Integer.parseInt(ConfigLoader.prop.getProperty("sf").split("x")[0]));
                new_dg.dataGenerator();
                // add loading code for the target DB here

            }
            else if(cmd.startsWith("run")) {
                String sqlsPath = argsList.get("f");
                SqlReader sqlStmt = new SqlReader(sqlsPath);
                bench.setSqls(sqlStmt.loader());

                if(cmd.equalsIgnoreCase("runLagtime")){
                    type=1;
                    bench.runLagTime();
                    //bench.res.setlagtime();
                    bench.getRes().printResult(type);
                }

                else if(cmd.equalsIgnoreCase("runElastic")){
                    type=4;
                    bench.runElastic(config);
                }

                else if(cmd.equalsIgnoreCase("runTenancy")){
                    bench.runTenancy(config);
                }

                else if(cmd.equalsIgnoreCase("runReplica")) {
                    type = 2;
                    bench.runReplica(type);
                    bench.res.printResult(type);
                }

                else if(cmd.equalsIgnoreCase("runScaling")) {
                    bench.runScaling(config);
                }

                else if(cmd.equalsIgnoreCase("runFailOver")) {
                    bench.runFailover();
                }

                else if(cmd.equalsIgnoreCase("runAll")){
                    // P-Score
                    bench.runReplica(2);
                    bench.getRes().printResult(2);
                    // E1-Score
                    bench.runElastic(config);
                    // E2-Score
                    bench.runScaling(config);
                    // R-Score and F-Score
                    bench.runFailover();
                    // T-Score
                    bench.runTenancy(config);
                    // C-Score
                    bench.runLagTime();
                    bench.getRes().printResult(1);

                    System.out.println("-----------Summary---------------------");
                    System.out.printf("The P-Score is   : %10.10f \n", bench.res.getP_SCORE());

                    System.out.printf("The E1-Score is   : %10.10f \n", bench.res.getE1_SCORE());

                    System.out.printf("The R-Score is   : %d \n", bench.res.getR_Score());

                    System.out.printf("The F-Score is   : %d \n", bench.res.getF_Score());

                    System.out.printf("The E2-Score is   : %10.10f \n", bench.res.getE2_SCORE());

                    System.out.println("The C-Score is "+ bench.res.getC_Score());

                    System.out.printf("The T-Score is   : %10.10f  \n", bench.res.getT_Score());

                    System.out.printf("The O-Score is   : %10.10f \n",
                            Math.log10((bench.res.getP_SCORE() * bench.res.getE1_SCORE() * bench.res.getE2_SCORE()*bench.res.getT_Score())/( bench.res.getR_Score()*bench.res.getF_Score()*bench.res.getC_Score()))
                    );
                }

                else{
                    logger.error("Run task not found : " + cmd);
                    cmdProcessor.printHelp();
                    System.exit(-1);
                }
                logger.info("Congs~ Test is done! Bye!");
            }
            else{
                logger.error("Not a known test type,please check");
                cmdProcessor.printHelp();
                System.exit(-1);
            }
        }
        else{
            cmdProcessor.printHelp();
            System.exit(-1);
        }
    }

}
