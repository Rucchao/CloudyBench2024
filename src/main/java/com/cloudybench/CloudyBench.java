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

import com.cloudybench.dbconn.ConnectionMgr;
import com.cloudybench.load.DataGenerator_Sales;
import com.cloudybench.load.ExecSQL;
import com.cloudybench.stats.Result;
import com.cloudybench.workload.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
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

    // run TP type workload. Spouse nums of threads defined in conf file.
    public void runTP(){
        logger.info("Begin TP Workload");
        taskType = 1;
        res.setStartTS(dateFormat.format(new Date()));
        String tpClient = ConfigLoader.prop.getProperty("tpclient");

        List<Client> tasks = new ArrayList<Client>();
        if(Integer.parseInt(tpClient) > 0){
            Client job = Client.initTask(ConfigLoader.prop,"TPClient",taskType);
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

    public void runAPower(){
        logger.info("Begin AP Workload");
        taskType = 2;
        res.setStartTS(dateFormat.format(new Date()));
        String apClient = "1";
        Client job = Client.initTask(ConfigLoader.prop,"APClient",taskType);
        job.setRet(res);
        job.setSqls(sqls);
        job.setVerbose(verbose);
        ExecutorService es = Executors.newFixedThreadPool(1);
        Future future = es.submit(new Runnable() {
            public void run() {
                // TODO Auto-generated method stub
                job.startTask();
            }}
        );

        if (future != null && !future.isCancelled() && !future.isDone()) {
            try {
                future.get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }

        if (!es.isShutdown() || !es.isTerminated()) {
            es.shutdownNow();
        }
        res.setEndTs(dateFormat.format(new Date()));
        logger.info("AP Workload is done.");
    }

    public void runAP(){
        logger.info("Begin AP Workload");
        taskType = 7;
        res.setStartTS(dateFormat.format(new Date()));
        String apClient = ConfigLoader.prop.getProperty("apclient");

        Client job = Client.initTask(ConfigLoader.prop,"APClient",taskType);
        job.setRet(res);
        job.setSqls(sqls);
        job.setVerbose(verbose);
        ExecutorService es = Executors.newFixedThreadPool(1);
        Future future = es.submit(new Runnable() {
            public void run() {
                // TODO Auto-generated method stub
                job.startTask();
            }}
        );

        if (future != null && !future.isCancelled() && !future.isDone()) {
            try {
                future.get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }

        if (!es.isShutdown() || !es.isTerminated()) {
            es.shutdownNow();
        }
        res.setEndTs(dateFormat.format(new Date()));
        logger.info("AP Workload is done.");
    }

    public void runXP(int tt){
        logger.info("Begin XP Workload");
        taskType = tt;
        res.setStartTS(dateFormat.format(new Date()));
        String tpClient = ConfigLoader.prop.getProperty("xtpclient","-1");
        if(tpClient.equalsIgnoreCase("-1")) {
            logger.error("Missing configuration xtpclient");
            System.exit(-1);
        }
        String apClient = ConfigLoader.prop.getProperty("xapclient","-1");
        if(tpClient.equalsIgnoreCase("-1")) {
            logger.error("Missing configuration xapclient");
            System.exit(-1);
        }

        List<Client> tasks = new ArrayList<Client>();
        if(Integer.parseInt(tpClient) > 0){
            Client job = Client.initTask(ConfigLoader.prop,"TPClient",taskType);
            job.setRet(res);
            job.setVerbose(verbose);
            job.setSqls(sqls);
            tasks.add(job);
        }

        if(Integer.parseInt(apClient) > 0){
            Client job = Client.initTask(ConfigLoader.prop,"APClient",taskType);
            job.setRet(res);
            job.setVerbose(verbose);
            job.setSqls(sqls);
            tasks.add(job);
        }

        ExecutorService es = Executors.newFixedThreadPool(tasks.size());
        List<Future> future = new ArrayList<Future>();
        for (final Client j : tasks) {
            future.add(es.submit(new Runnable() {
                public void run() {
                    j.startTask();
                }})
            );
        }
        Thread freshness = null;
        if(taskType == 4){
            final long startTs = System.currentTimeMillis();
            //System.out.println("Start time is "+Instant.now());
            //final long startTs = Instant.now().toEpochMilli();
            final int _duration = Integer.parseInt(ConfigLoader.prop.getProperty("xpRunMins"));
            final int _fresh_interval = Integer.parseInt(ConfigLoader.prop.getProperty("fresh_interval",String.valueOf(20)));
            String db = ConfigLoader.prop.getProperty("db");
            int dbType = Constant.getDbType(db);
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

        if(freshness != null){
            freshness.interrupt();
        }

        if (!es.isShutdown() || !es.isTerminated()) {
            es.shutdownNow();
        }
        res.setEndTs(dateFormat.format(new Date()));
        logger.info("XP Workload is done");
    }

    public void runFreshness(int tt){

        logger.info("Begin Freshness Workload");
        runXP(tt);
        logger.info("Freshness Workload is done.");
    }

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

    public static void main(String[] args) throws SQLException {
        LoggerContext context = (LoggerContext) LogManager.getContext(false);
        File file = new File("conf/log4j2.properties");
        context.setConfigLocation(file.toURI());
        ConfigLoader config = new ConfigLoader();
        CloudyBench hybench = new CloudyBench();
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
            String testTime=config.prop.getProperty("testTime");
            total_test_time=Integer.parseInt(testTime);
            config.printConfig();
        }

        if(argsList.containsKey("s")){
            hybench.verbose = false;
        }

        if(argsList.containsKey("m")){
            String num = argsList.get("m");
            hybench.TP_tenant_num = Integer.parseInt(num);
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
                hybench.setSqls(sqlStmt.loader());

                if(cmd.equalsIgnoreCase("runxp") ){
                    type=0;
                    hybench.runXP(0);
                }
                else if(cmd.equalsIgnoreCase("runtp")){
                    type=1;
                    hybench.runTP();
                }
                else if(cmd.equalsIgnoreCase("runap")){
                    type=7;
                    hybench.runAP();
                }
                else if(cmd.equalsIgnoreCase("runappower")){
                    type=2;
                    hybench.runAPower();
                }
                else if(cmd.equalsIgnoreCase("runhtap")){
                    type=3;
                    hybench.runAP();
                    hybench.runTP();
                    hybench.runXP(0);
                }
                else if(cmd.equalsIgnoreCase("runFresh")){
                    type=4;
                    hybench.runFreshness(4);
                }
                else if(cmd.equalsIgnoreCase("runAll")){
                    type=6;
                    hybench.runAP();
                    hybench.runTP();
                    hybench.runFreshness(4);
                }
                else if(cmd.equalsIgnoreCase("runCloudTP")){
                    type=8;

                    // Workload Pattern Generation
                    Con= new int[total_test_time][hybench.TP_tenant_num];

                    // the concurrency in the first minute
                    Con[0][0]=5;
                    //Con[0][1]=5;


                    // the concurrency in the second minute
                    Con[1][0]=5;
                    //Con[1][1]=5;


                    // the concurrency in the third minute
                    Con[2][0]=5;
                    //Con[2][1]=5;


                    for (int i = 1; i <= total_test_time; i++) {
                        logger.info("This is the "+i+"-th time slot...");
                        hybench.runCloudTP(hybench.TP_tenant_num, Con[i-1]);
                        // if( hybench.getRes().getTpsList() != null && Con[i - 1][0] > 0)
                        for (int j = 0; j < hybench.TP_tenant_num; j++) {
                            if (Con[i - 1][j] != 0) {
                                hybench.getRes().printResult(type);
                                break;
                            }
                        }
                        // if( hybench.getRes().getTpsList() != null)
                        //     hybench.getRes().printResult(type);
                    }
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
