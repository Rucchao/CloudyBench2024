package com.hybench;
/**
 *
 * @time 2023-03-04
 * @version 1.0.0
 * @file Hybench.java
 * @description
 *      Here is the main class. Load configuration from conf file and read options from command line.
 *      Four different test types are provided, including runAP, runTP, runXP ,runHTAP and runAll.
 **/

import com.hybench.dbconn.ConnectionMgr;
import com.hybench.load.DataGenerator_RiskControlling;
import com.hybench.load.ExecSQL;
import com.hybench.stats.Result;
import com.hybench.workload.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class HyBench {
    public static Logger logger = LogManager.getLogger(HyBench.class);
    int taskType = 0;
    Result res = new Result();
    boolean verbose = true;
    int tenant_num=0;
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
            freshness = new Thread(){

                public void run() {
                    Connection conn_tp = ConnectionMgr.getConnection(0);
                    Connection conn_ap = ConnectionMgr.getConnection(1);
                    double sum = 0;
                    int cnt = 0;
                    long duration = _duration * 60 * 1000L;
                    long elpased_time = 0L;
                    boolean hasGetFreshness = false;
                    for (int i = 0; i < _fresh_interval; i++) {
                        try {
                            Thread.sleep(_duration * 60 * 1000/_fresh_interval);
                            elpased_time += _duration * 60 * 1000/_fresh_interval;
                            Freshness2 fresh = new Freshness2(dbType,conn_tp,conn_ap,sqls,startTs);
                            if(fresh.calcFreshness() == 2147483647){
                                continue;
                            }
                            sum += fresh.calcFreshness();
                            cnt++;
                            res.setFresh(sum/cnt * 1.0);
                            hasGetFreshness=true;
                        } catch (InterruptedException e) {
                            logger.info("Freshness checker was stopped in force");
                        } catch (Exception e){
                            e.printStackTrace();
                        }
                    }
                    if(!hasGetFreshness){
                        res.setFresh(2147483647);
                    }
                    try {
                        if(conn_ap != null) {
                            conn_ap.close();
                        }
                        if(conn_tp != null) {
                            conn_tp.close();
                        }
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            };
            freshness.start();
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

    public void runCloudTP(int tenant_num){
        logger.info("Begin Cloud TP Workload");
        taskType = 8;
        List<Client> tasks = new ArrayList<Client>();
        res.setStartTS(dateFormat.format(new Date()));
        res.setTenant_num(tenant_num);
        for (int i = 1; i <=tenant_num ; i++) {
            String tpClient = ConfigLoader.prop.getProperty("tpclient_"+i);
            if(Integer.parseInt(tpClient) > 0){
                Client job = Client.initTask(ConfigLoader.prop,"CloudTPClient",taskType, i);
                //Result res_tenant = new Result(i);
                job.setRet(res);
                job.setVerbose(verbose);
                job.setSqls(sqls);
                tasks.add(job);
            }
            else {
                logger.warn("There is no an available tp client");
                return;
            }
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

    public void runCloudAP(int tenant_num){
        logger.info("Begin Cloud AP Workload");
        taskType = 9;
        List<Client> tasks = new ArrayList<Client>();
        res.setStartTS(dateFormat.format(new Date()));
        res.setTenant_num(tenant_num);
        for (int i = 1; i <=tenant_num ; i++) {
            String apClient = ConfigLoader.prop.getProperty("apclient_"+i);
            if(Integer.parseInt(apClient) > 0){
                Client job = Client.initTask(ConfigLoader.prop,"CloudAPClient",taskType, i);
                //Result res_tenant = new Result(i);
                job.setRet(res);
                job.setVerbose(verbose);
                job.setSqls(sqls);
                tasks.add(job);
            }
            else {
                logger.warn("There is no an available tp client");
                return;
            }
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


        logger.info("Cloud AP Workload is done.");
    }

    public static void main(String[] args) throws SQLException {
        LoggerContext context = (LoggerContext) LogManager.getContext(false);
        File file = new File("conf/log4j2.properties");
        context.setConfigLocation(file.toURI());
        ConfigLoader config = new ConfigLoader();
        HyBench hybench = new HyBench();

        logger.info("Hi~Bench, HyBench");
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
            hybench.verbose = false;
        }

        if(argsList.containsKey("m")){
            String num = argsList.get("m");
            hybench.tenant_num = Integer.parseInt(num);
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
                DataGenerator_RiskControlling new_dg = new DataGenerator_RiskControlling((ConfigLoader.prop.getProperty("sf")));
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
                    hybench.runCloudTP(hybench.tenant_num);
                }
                else if(cmd.equalsIgnoreCase("runCloudAP")){
                    type=9;
                    hybench.runCloudAP(hybench.tenant_num);
                }
                else{
                    logger.error("Run task not found : " + cmd);
                    cmdProcessor.printHelp();
                    System.exit(-1);
                }
                logger.info("Congs~ Test is done! Bye!");
                hybench.getRes().printResult(type);
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
