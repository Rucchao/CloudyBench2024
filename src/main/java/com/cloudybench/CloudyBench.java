package com.cloudybench;
/**
 *
 * @time 2023-03-04
 * @version 1.0.0
 * @file Hybench.java
 * @description
 *      Here is the main class. Load configuration from conf file and read options from command line.
 *      Four different test types are provided, including runAP, runTP, runXP ,runHTAP and runAll.
 **/

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
    public void runFailover(int taskType){
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

    public static void main(String[] args) throws SQLException, ParseException, IOException {
        LoggerContext context = (LoggerContext) LogManager.getContext(false);
        File file = new File("conf/log4j2.properties");
        context.setConfigLocation(file.toURI());
        ConfigLoader config = new ConfigLoader();
        CloudyBench bench = new CloudyBench();
        int[][] Con=null;
        logger.info("Hi~ this is CloudyBench");
        int total_test_time = 0;
        int first_con=0;
        int second_con=0;
        int third_con=0;
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

                    String elastic_testTime=config.prop.getProperty("elastic_testTime");
                    String first_con_str=config.prop.getProperty("first_con");
                    String second_con_str=config.prop.getProperty("second_con");
                    String third_con_str=config.prop.getProperty("third_con");
                    total_test_time = Integer.parseInt(elastic_testTime);
                    first_con = Integer.parseInt(first_con_str);
                    second_con = Integer.parseInt(second_con_str);
                    third_con = Integer.parseInt(third_con_str);

                    // Workload Pattern Generation
                    Con= new int[total_test_time][bench.TP_tenant_num];

                    // the concurrency in the first minute
                    Con[0][0]=first_con;
                    //Con[0][1]=5;

                    // the concurrency in the second minute
                    Con[1][0]=second_con;
                    //Con[1][0]=5; //tenant 0
                    //Con[1][1]=5; // tenant 1

                    // the concurrency in the third minute
                    Con[2][0]=third_con;
                    //Con[2][0]=5;
                    //Con[2][1]=5;

                    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    String StartTime = dateFormat.format(System.currentTimeMillis());

                    logger.info("Elastic test starts at " + StartTime);
                    double total_tps=0;


                    for (int i = 1; i <= total_test_time; i++) {
                            logger.info("This is the "+i+"-th time slot...");
                            bench.runCloudTP(bench.TP_tenant_num, Con[i-1]);
                            // if( hybench.getRes().getTpsList() != null && Con[i - 1][0] > 0)
                            Result res= bench.getRes();
                            double tps=res.getTpsList()[0];
                            total_tps+=tps;
                            //res.printResult(type);
                        }
                    // need to compute the average TPS for three minutes
                    System.out.println("====================Elasticity Summary========================");

                    double avg_tps=total_tps/3;

                    System.out.printf("The elastic average tps is  : %10.2f \n", avg_tps  * 1.0);

                    String cdb = config.prop.getProperty("cdb","neon");

                    if(cdb.equals("neon")){
                        // caculate the resources
                        NeonAPI neon = new NeonAPI();
                        String json=neon.metricJson(StartTime);
                        String url = config.prop.getProperty("metric_url","404");
                        double cpus = neon.doPostRequest(url,json);
                        double rcu_c = Double.parseDouble(config.prop.getProperty("rcu_c","0"));
                        double rcu_m = Double.parseDouble(config.prop.getProperty("rcu_m","0"));
                        int cpu_mem_ratio=Integer.parseInt(config.prop.getProperty("cpu_mem_ratio","1"));
                        double resource_cost=cpus * rcu_c+cpus * rcu_m * cpu_mem_ratio;
                        System.out.println("-----------E1-Score--------------------");
                        System.out.printf("The E1-Score is   : %10.2f \n", (avg_tps/resource_cost)  * 1.0);
                    }
                }

                else if(cmd.equalsIgnoreCase("runTenancy")){
                    String elastic_testTime=config.prop.getProperty("elastic_testTime");
                    total_test_time = Integer.parseInt(elastic_testTime);

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


                    // Workload Pattern Generation
                    Con= new int[total_test_time][bench.TP_tenant_num];

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

                    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    String StartTime = dateFormat.format(System.currentTimeMillis());

                    logger.info("Multi-tenancy test starts at " + StartTime);
                    double total_tenant1_tps=0;
                    double total_tenant2_tps=0;
                    double total_tenant3_tps=0;

                    for (int i = 1; i <= total_test_time; i++) {
                        logger.info("This is the " + i + "-th time slot...");
                        bench.runCloudTP(bench.TP_tenant_num, Con[i - 1]);
                        for (int j = 0; j < bench.TP_tenant_num; j++) {
                            if (Con[i - 1][j] != 0) {
                                //bench.getRes().printResult(type);
                                Result res= bench.getRes();
                                total_tenant1_tps += res.getTpsList()[0];
                                total_tenant2_tps += res.getTpsList()[1];
                                total_tenant3_tps += res.getTpsList()[2];
                                break;
                            }
                        }
                    }

                    System.out.println("====================Multi-Tenancy Summary========================");

                    double geomean_tps=Math.pow(total_tenant1_tps/3*total_tenant2_tps/3*total_tenant3_tps/3, 1/3.0);

                    System.out.printf("The multitenancy average tps is  : %10.2f \n",geomean_tps * 1.0);

                    String cdb = config.prop.getProperty("cdb","neon");

                    if(cdb.equals("neon")){
                        // caculate the total resource cost
                        NeonAPI neon = new NeonAPI();
                        String json=neon.metricJson(StartTime);
                        double rcu_c = Double.parseDouble(config.prop.getProperty("rcu_c","0"));
                        double rcu_m = Double.parseDouble(config.prop.getProperty("rcu_m","0"));
                        int cpu_mem_ratio=Integer.parseInt(config.prop.getProperty("cpu_mem_ratio","1"));

                        // tenant 1
                        String url_1 = config.prop.getProperty("metric_url_1","404");
                        double cpus_1 = neon.doPostRequest(url_1,json);

                        // tenant 2
                        String url_2 = config.prop.getProperty("metric_url_2","404");
                        double cpus_2 = neon.doPostRequest(url_2,json);

                        // tenant 3
                        String url_3 = config.prop.getProperty("metric_url_3","404");
                        double cpus_3 = neon.doPostRequest(url_3,json);

                        double resource_cost=cpus_1 * rcu_c+cpus_1 * rcu_m * cpu_mem_ratio
                                +cpus_2 * rcu_c+cpus_2 * rcu_m * cpu_mem_ratio
                                +cpus_3 * rcu_c+cpus_3 * rcu_m * cpu_mem_ratio;
                        System.out.println("-----------T-Score--------------------");
                        System.out.printf("The total resource cost is   : %10.2f \n", resource_cost);
                        System.out.printf("The T-Score is   : %10.10f \n", (2.14/resource_cost* 1.0) );
                    }
                }

                else if(cmd.equalsIgnoreCase("runReplica")) {
                    type = 2;
                    bench.runReplica(type);
                    //bench.res.setlagtime();
                    //bench.getRes().printResult(type);
                }

                else if(cmd.equalsIgnoreCase("runScaling")) {
                    type = 3;
                    System.out.println("====================The RW is running========================");
                    bench.runReplica(type);
                    System.out.println("====================The RW and RO are running========================");
                    bench.runReplica(2);

                    Result res= bench.getRes();

                    System.out.println("====================Scaling Summary========================");

                    int replica_num=Integer.parseInt(config.prop.getProperty("node_num","1"));

                    System.out.printf("The RW average tps is  : %10.2f \n", res.getTps_rw()  * 1.0);

                    System.out.printf("The RW-RO average tps is  : %10.2f \n", res.getTps_ro()  * 1.0);

                    System.out.println("-----------E2-Score--------------------");
                    System.out.printf("The E2-Score is   : %10.2f \n", (res.getTps()/(res.getTps_rw()*replica_num))  * 1.0);
                }

                else if(cmd.equalsIgnoreCase("runFailOver")) {
                    // RW-RO
                    bench.runFailover(5);
                    // RW
                    bench.runFailover(4);
                    // Results
                    Result res= bench.getRes();

                    System.out.println("====================Failover Summary========================");

                    System.out.println("-----------F-Score--------------------");
                    System.out.println(res.getF_Score_RO());
                    System.out.println(res.getF_Score_RW());
                    System.out.printf("The F-Score is  : %d \n", (res.getF_Score_RO()+res.getF_Score_RW())/2);

                    System.out.println("-----------R-Score--------------------");
                    System.out.println("====================Failover Summary========================");
                    System.out.println(res.getR_Score_RO());
                    System.out.println(res.getR_Score_RW());
                    System.out.printf("The R-Score is  : %d \n", (res.getR_Score_RO()+res.getR_Score_RW())/2);
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
