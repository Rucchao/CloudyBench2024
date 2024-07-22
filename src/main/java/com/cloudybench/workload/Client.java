package com.cloudybench.workload;
/**
 *
 * @time 2023-03-04
 * @version 1.0.0
 * @file Client.java
 * @description
 *   abstract class Client
 **/

import com.cloudybench.Constant;
import com.cloudybench.load.ConfigReader;
import com.cloudybench.stats.Histogram;
import com.cloudybench.stats.Result;
import com.cloudybench.util.NeonAPI;
import com.cloudybench.util.RandomGenerator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.io.*;
import com.cloudybench.ConfigLoader;

public abstract class Client {
    public static Logger logger = LogManager.getLogger(Client.class);

    protected boolean exitFlag = false;
    protected Properties prop = null;
    CompletionService<ClientResult> cs = null;
    Future<ClientResult>[] fs = null;
    int dbType = 0;
    Sqlstmts sqls = null;
    private int testDuration = 0;
    private String clientName = "";
    int threads = 0;
    static long tpTotalCount = 0L;
    static int[] tpTotalList;
    static double[] tpsList;
    static int[] apTotalList;
    static double[] apsList;
    ArrayList<Long> lagtime;
    Lock lock = new ReentrantLock();
    protected int taskType = 0;
    ConfigReader CR = null;
    Result ret = null;
    Histogram hist = null;
    boolean verbose = true;
    int round = 1;
    static int testid1=1;
    static int testid2=300001;
    static int testid3=300001;
    long F_Score=0;
    long R_Score=0;
    int tenant_num=0;

    int num = 0;
    RandomGenerator rg = new RandomGenerator();


    ExecutorService es = null;//Executors.newFixedThreadPool(5);

    public void setNum(int num) {
        this.num = num;
    }

    public void setTenant_num(int tenant_num){
        this.tenant_num = tenant_num;
    }

    public void setVerbose(boolean verbose){
        this.verbose = verbose;
    }

    public boolean isVerbose() {
        return verbose;
    }

    public void setSqls(Sqlstmts sqls) {
        this.sqls = sqls;
    }

    public void setRet(Result ret) {
        this.ret = ret;
    }

    public Result getRet() {
        return ret;
    }

    public void setDbType(int dbType){
        this.dbType = dbType;
    }

    public void setTestTime(int time) {
        testDuration = time;
    }
    public int getTestTime() {
        return testDuration;
    }

    public void setTaskType(int type){
        this.taskType = type;
    }

    public int getTaskType(){
        return taskType;
    }

    public void setClientName(String name){
        this.clientName = name;
    }

    public String getClientName(){
        return this.clientName;
    }

    protected void setTask_prop(Properties prop) {
        this.prop = prop;
    }

    // parameter handler including string value , int value and bool value
    protected String strParameter(String paraName) {
        return prop.containsKey(paraName) ? prop
                .getProperty(paraName) : null;
    }

    protected String strParameter(String paraName, String defaultValue) {
        return prop.containsKey(paraName) ? prop
                .getProperty(paraName) : defaultValue;
    }

    protected int intParameter(String paraName, int defaultValue) {
        String v = prop
                .getProperty(paraName, String.valueOf(defaultValue));
        return Integer.parseInt(v);
    }

    protected int intParameter(String paraName) {

        return intParameter(paraName, 0);
    }

    protected boolean boolParameter(String paraName, boolean defaultValue) {
        if (prop.containsKey(paraName)) {
            if ("true".equalsIgnoreCase(prop.getProperty(paraName))) {
                return true;
            }
            return false;
        }
        return defaultValue;
    }

    protected boolean boolParameter(String paraName) {
        return boolParameter(paraName, false);
    }

    // get db type
    public int getDbType(String db){

        if(db.equalsIgnoreCase("postgreSQL")){
            return Constant.DB_PG;
        }
        else if(db.equalsIgnoreCase("mysql")){
            return Constant.DB_MYSQL;
        }
        else if(db.equalsIgnoreCase("oracle")){
            return Constant.DB_ORACLE;
        }
        else{
            return Constant.DB_UNKNOW;
        }
    }
    // get data size and create thread pool according to client number
    public void doInit_wrapper(String clientName, int concurrency) {
        if(taskType == 1){
            threads = intParameter("tpclient");
            lagtime= new ArrayList<Long>();
        }

        else if(taskType == 2 || taskType == 3 ){
            tpTotalCount = 0;
            threads = intParameter("tpclient");
        }
        else if (taskType == 4 || taskType == 5)
            threads = intParameter("tpclient");

        else if(taskType == 8){
            threads = concurrency;
            tpTotalList=new int[tenant_num];
            tpsList=new double[num];
        }

        CR = new ConfigReader("cloudybench");
        String db = strParameter("db");
        setDbType(getDbType(db));

        if (threads > 0) {
            logger.info("The number of threads is "+ threads);
            es = Executors.newFixedThreadPool(threads);
            cs = new ExecutorCompletionService<ClientResult>(es);
        }

        doInit();
    }

    public static Client initTask(Properties cfg,String name,int taskType) {
        Client client = null;
        try {
            client = (Client) Class.forName("com.cloudybench.workload." + name).getDeclaredConstructor().newInstance();
            client.setClientName(name);
            client.setTask_prop(cfg);
            client.setTaskType(taskType);
            client.doInit_wrapper(name,0);
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return client;
    }

    public static Client initTask(Properties cfg,String name,int taskType, int tenant_num, int concurrency, int num) {
        Client client = null;
        try {
            client = (Client) Class.forName("com.cloudybench.workload." + name).getDeclaredConstructor().newInstance();
            client.setClientName(name+tenant_num);
            client.setTask_prop(cfg);
            client.setTaskType(taskType);
            client.setTenant_num(tenant_num);
            client.setNum(num);
            client.doInit_wrapper(name+tenant_num, concurrency);
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return client;
    }

    // client work from here. A new thread named timer to output response time histogram every 1/10 duration
    public void startTask() {
        int testTime = 0;
        ClientResult _res = null;
        Thread timer = null;
        String db = strParameter("db");
        setDbType(getDbType(db));
        if(ret != null){
            hist = ret.getHist();
        }

         if (taskType == 1 || taskType == 8 || taskType == 2 || taskType == 3  || taskType == 4|| taskType == 5){
            testTime = intParameter("tpRunMins");
            ret.setTpclient(threads);
        }

        final int _duration = testTime;

        logger.info("Begin to run :" + clientName + ", Test Duration is "  + _duration + " mins");

        setTestTime(testTime);

        if (_duration > 0) {
            timer = new Thread() {
                public void run() {
                    try {
                        long duration = _duration * 60 * 1000L;
                        long elpased_time = 0L;
                        long TCount=0;
                        long interval=0;
                        long threshold=0;
                        boolean recovery_service=false;
                        boolean recovery_tps=false;
                        for (int i = 0; i < 10; i++) {
                            if(clientName.equalsIgnoreCase("CloudFailover")) {
                                System.out.println("The current i is "+i);
                                System.out.println("The before count is "+tpTotalCount);
                            }
                            TCount=tpTotalCount;
                            // the transactions are processing here
                            Thread.sleep(_duration * 60 * 100L);

                            elpased_time += _duration * 60 * 100L;

                            // injecting the failure point
                            if(clientName.equalsIgnoreCase("CloudFailover")){
                                System.out.println("The after count is "+tpTotalCount);
                                interval=tpTotalCount-TCount;
                                System.out.println("The current interval is >>>>>>>>>>>>>>"+interval);
                                if(taskType==4 && i==2)
                                {
                                    // restarting the RW endpoint
                                    ConfigLoader config = new ConfigLoader();
                                    String cdb = config.prop.getProperty("cdb","neon");
                                    String endpoint = config.prop.getProperty("RW_endpoint","neon");
                                    if(cdb.equals("neon")){
                                        NeonAPI neon = new NeonAPI();
                                        String key = config.prop.getProperty("authentication_key");
                                        neon.Endpoint(endpoint,2, key);
                                        threshold= Integer.parseInt(config.prop.getProperty("tps"));
                                    }
                                    System.out.println("This is a RW failure point !!");
                                }

                                if(taskType==5 && i==2)
                                {
                                    // restarting the RW endpoint
                                    ConfigLoader config = new ConfigLoader();
                                    String cdb = config.prop.getProperty("cdb","neon");
                                    String endpoint = config.prop.getProperty("RO_endpoint","neon");
                                    if(cdb.equals("neon")){
                                        NeonAPI neon = new NeonAPI();
                                        String key = config.prop.getProperty("authentication_key");
                                        neon.Endpoint(endpoint,2, key);
                                        threshold= Integer.parseInt(config.prop.getProperty("tps"));
                                    }
                                    System.out.println("This is a RO failure point !!");
                                }
                                // calculate the F-Score in seconds
                                if(i>2 && interval>=0 && recovery_service==false){
                                    recovery_service=true;
                                    F_Score=_duration * 6 * (i-2);
                                    System.out.println("The F Score is "+F_Score);
                                }

                                // calculate the R-Score in seconds
                                if(i>2 && interval>=threshold && recovery_tps==false){
                                    recovery_tps=true;
                                    R_Score=_duration * 6 * (i-2);
                                    System.out.println("The R Score is "+R_Score);
                                }
                                if(i==9 && recovery_tps==false){
                                    R_Score=_duration * 6 * (i-2);
                                    System.out.println("The R Score is "+R_Score);
                                }
                            }
                            if(verbose){
                                if(clientName.equalsIgnoreCase("CloudLagTime") || clientName.equalsIgnoreCase("CloudReplica") ) {
                                    for(int tpidx = 0;tpidx < 4;tpidx++) {
                                        if(clientName.equalsIgnoreCase("CloudLagTime") && tpidx == 2 ){
                                            continue;
                                        }
                                        if(hist.getTPItem(tpidx).getN() == 0)
                                            continue;
                                        logger.info("Transaction " + (tpidx+1)
                                                + " : max rt : " + hist.getTPItem(tpidx).getMax()
                                                + " | min rt :" + hist.getTPItem(tpidx).getMin()
                                                + " | avg rt : " + String.format("%.2f",hist.getTPItem(tpidx).getMean())
                                                + " | 95% rt : " + String.format("%.2f",hist.getTPItem(tpidx).getPercentile(95))
                                                + " | 99% rt : " + String.format("%.2f",hist.getTPItem(tpidx).getPercentile(99)));
                                    }
                                    logger.info("Current " + (i + 1) + "/10 time TP TPS is " + String.format("%.2f", tpTotalCount / (elpased_time / 1000.0)));


                                }
                                if(clientName.equalsIgnoreCase("CloudTPClient"+tenant_num)) {
                                    for(int tpidx = 0;tpidx < 3;tpidx++) {
                                        if(hist.getTPItem(tpidx).getN() == 0)
                                            continue;
//                                        logger.info("Transaction " + (tpidx+1)
//                                                + " : max rt : " + hist.getTPItem(tpidx).getMax()
//                                                + " | min rt :" + hist.getTPItem(tpidx).getMin()
//                                                + " | avg rt : " + String.format("%.2f",hist.getTPItem(tpidx).getMean())
//                                                + " | 95% rt : " + String.format("%.2f",hist.getTPItem(tpidx).getPercentile(95))
//                                                + " | 99% rt : " + String.format("%.2f",hist.getTPItem(tpidx).getPercentile(99)));
                                    }
                                    if(taskType == 8) {
                                        logger.info("This is tenant "+tenant_num);
                                        logger.info("Client"+tenant_num+" : Current " + (i + 1) + "/10 time TP TPS is " + String.format("%.2f", tpTotalList[tenant_num-1] / (elpased_time / 1000.0)));
                                    }
                                }
                            }
                        }

                        stopTask();

                    } catch (InterruptedException e) {
                        logger.warn("Test Duration Timer was stopped in force");
                        return;
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            };
            timer.start();
        }

        int _num_thread = threads;

        fs = new Future[_num_thread];
        for (int i = 1; i <= _num_thread; i++) {
            final String threadId = "T" + i;
            Callable<ClientResult> r = new Callable<ClientResult>() {

                public ClientResult call() throws Exception {
                    // TODO Auto-generated method stub
                    Thread.currentThread().setName(threadId);
                    ClientResult result= execute();
                    return result;
                }
            };
            try {
                fs[i-1] = cs.submit(r);
            }  catch(Exception e) {
                logger.error("create thread failed " ,e );
            }
        }



        double maxElapsedTime = 0L;
        for (int i = 0; i < _num_thread; i++) {
            try {
                fs[i] = cs.take();
                if(fs[i] != null && !fs[i].isCancelled() && fs[i].isDone() ) {
                    _res = fs[i].get();
                    if(_res.getRt() > maxElapsedTime)
                        maxElapsedTime = _res.getRt();
                }
            } catch (Exception e) {
                logger.error("Waiting for load worker", e);
            }
        }

        if(clientName.equalsIgnoreCase("CloudLagTime")) {
            if (taskType == 1) {
                ret.setTpTotal(tpTotalCount);
                ret.setlagList(lagtime);
                ret.setTps(Double.valueOf(String.format("%.2f", tpTotalCount / (testDuration * 60.0))));
            }
        }

        if(clientName.equalsIgnoreCase("CloudReplica")) {
            if (taskType == 2) {
                ret.setTpTotal(tpTotalCount);
                ret.setTps(Double.valueOf(String.format("%.2f", tpTotalCount / (testDuration * 60.0))));
                ret.setTps_ro(Double.valueOf(String.format("%.2f", tpTotalCount / (testDuration * 60.0))));
            }

            if (taskType == 3) {
                ret.setTpTotal(tpTotalCount);
                ret.setTps_rw(Double.valueOf(String.format("%.2f", tpTotalCount / (testDuration * 60.0))));
            }
        }

        if(clientName.equalsIgnoreCase("CloudTPClient"+tenant_num)) {

            if (taskType == 8) {
                ret.setTpTotalList(tpTotalList);

                tpsList[tenant_num-1]=Double.valueOf(String.format("%.2f", tpTotalList[tenant_num-1] / (testDuration * 60.0)));
                ret.setTpsList(tpsList);
                //ret.setTps(Double.valueOf(String.format("%.2f", tpTotalList[tenant_num-1] / (testDuration * 60.0))));
            }
        }

        if(clientName.equalsIgnoreCase("CloudFailover")){
            logger.info("The task is hanging...");
            if (taskType == 4) {
                ret.setF_Score_RW(F_Score);
                ret.setR_Score_RW(R_Score);
                ret.setTpTotal(tpTotalCount);
                ret.setTps(Double.valueOf(String.format("%.2f", tpTotalCount / (testDuration * 60.0))));
                ret.setTps_ro(Double.valueOf(String.format("%.2f", tpTotalCount / (testDuration * 60.0))));
            }
            if (taskType == 5) {
                ret.setF_Score_RO(F_Score);
                ret.setR_Score_RO(R_Score);
                ret.setTpTotal(tpTotalCount);
                ret.setTps(Double.valueOf(String.format("%.2f", tpTotalCount / (testDuration * 60.0))));
                ret.setTps_ro(Double.valueOf(String.format("%.2f", tpTotalCount / (testDuration * 60.0))));
            }
        }

        if (timer != null) {
            timer.interrupt();
        }

        if (!es.isShutdown() || !es.isTerminated()) {
            es.shutdownNow();
        }
        logger.info( "Finished to execute " + clientName );
    }

    public abstract void doInit();

    public abstract ClientResult execute();

    public void stopTask() {
        exitFlag =  true;
        logger.info("The task is stopping...");
        if(fs != null){
            for(Future ft:fs){
                ft.cancel(true);
            }
        }
    }

    public ArrayList<Integer> getRandomList(int minValue, int maxValue){
        ArrayList<Integer> rList = new ArrayList<Integer>();
        LinkedList<Integer> source= new LinkedList<Integer>();
        for(int i=minValue;i<=maxValue;i++) {
            source.add(i);
        }
        while(source.size() > 0) {
            int idx = ThreadLocalRandom.current().nextInt(source.size());
            rList.add(source.get(idx));
            source.remove(idx);
        }
        return rList;
    }
}
