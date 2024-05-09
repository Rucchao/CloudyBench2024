package com.cloudybench.stats;

import com.cloudybench.ConfigLoader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;

/**
 *
 * @time 2023-03-04
 * @version 1.0.0
 * @file Result.java
 * @description
 *   record test result and print summary after all workloads are done.
 **/

public class Result {
    public static Logger logger = LogManager.getLogger(Result.class);
    private String dbType = null;
    private int tenant_num;
    private int[] tpTotalList;
    private double[] tpsList;
    private int[] apTotalList;
    private double[] apsList;
    private long tpTotal;
    private long apTotal;
    private double tps;
    private double qps;
    private String startTS ;
    private String endTs;
    private Histogram hist;
    private double freshness = 0;
    private ArrayList<Long> laglist;
    private long lagtime = 0;
    private int apclient ;
    private int tpclient;
    private int xapclient ;
    private int xtpclient;
    private String riskRate;
    private int apRound;

    public Result(int tenant_num){
        this.tenant_num=tenant_num;
    }

    public void setTenant_num(int num) {
        this.tenant_num = num;
    }

    public void setApRound(int round) {
        this.apRound = round;
    }

    public int getApRound() {
        return this.apRound;
    }

    public void setRiskRate(String riskRate) {
        this.riskRate = riskRate;
    }

    public String getRiskRate() {
        return this.riskRate;
    }

    public Result(){
        hist = new Histogram();
    }

    public void setHist(Histogram hist) {
        this.hist = hist;
    }

    public Histogram getHist() {
        return hist;
    }

    public void setApTotal(long apTotal) {
        this.apTotal = apTotal;
    }

    public void setDbType(String dbType) {
        this.dbType = dbType;
    }

    public void setEndTs(String endTs) {
        this.endTs = endTs;
    }

    public void setQps(double qps) {
        this.qps = qps;
    }

    public void setStartTS(String startTS) {
        this.startTS = startTS;
    }

    public void setTps(double tps) {
        this.tps = tps;
    }

    public void setTpTotal(long tpTotal) {
        this.tpTotal = tpTotal;
    }

    public void setTpTotalList(int[] TpTotalList) {
        this.tpTotalList = TpTotalList;
    }

    public double[] getTpsList() {
        return tpsList;
    }

    public void setTpsList(double[] TpsList) {
        this.tpsList = TpsList;
    }

    public void setlagList(ArrayList llist) {
        this.laglist = llist;
    }

    public long getLagtime(){
        if (laglist.size()==0)
            return 0;

        long sum = 0;
        for (int i = 0; i < laglist.size(); i++)
        {
            sum += laglist.get(i);
        }

        // Calculate the average of elements
        long average = sum / laglist.size();

        return average;
    }

    public void setapTotalList(int[] apTotalList) {
        this.apTotalList = apTotalList;
    }

    public void setapsList(double[] apsList) {
        this.apsList = apsList;
    }

    public int getApclient() {
        return apclient;
    }

    public void setApclient(int apclient) {
        this.apclient = apclient;
    }

    public int getTpclient() {
        return tpclient;
    }

    public void setTpclient(int tpclient) {
        this.tpclient = tpclient;
    }

    public int getXapclient() {
        return xapclient;
    }

    public void setXapclient(int xapclient) {
        this.xapclient = xapclient;
    }

    public int getXtpclient() {
        return xtpclient;
    }

    public void setXtpclient(int xtpclient) {
        this.xtpclient = xtpclient;
    }

    public String getDbType() {
        return dbType;
    }

    public long getTpTotal() {
        return tpTotal;
    }

    public long getApTotal() {
        return apTotal;
    }

    public double getTps() {
        return tps;
    }

    public double getQps() {
        return qps;
    }

    public String getStartTS() {
        return startTS;
    }

    public String getEndTs() {
        return endTs;
    }

    public double getFresh(){
        return freshness;
    }

    public void setlagtime(long lagtime){
        this.lagtime = lagtime;
    }

    public void printResult(int type){
        logger.info("====================Test Summary========================");
        logger.info("Test starts at " + getStartTS());
        logger.info("Test ends at " + getEndTs());
        Double rcu_c = Double.parseDouble(ConfigLoader.prop.getProperty("rcu_c","1"));
        Double rcu_m = Double.parseDouble(ConfigLoader.prop.getProperty("rcu_m","1"));
        int cpu_num = Integer.parseInt(ConfigLoader.prop.getProperty("cpu_num","1"));
        int mem_num = Integer.parseInt(ConfigLoader.prop.getProperty("mem_num","1"));
        int node_num = Integer.parseInt(ConfigLoader.prop.getProperty("node_num","1"));

        switch(type){
            case 1:
                logger.info("TP Concurrency is " + getTpclient());
                logger.info("Total amount of TP Transaction is " + getTpTotal());
                logger.info("TPS is " + getTps());
                break;
            case 2:
                logger.info("TP Concurrency is " + getTpclient());
                logger.info("Total amount of TP Transaction is " + getTpTotal());
                logger.info("TPS is " + getTps());
                break;
        }
        logger.info("Query/Transaction response time(ms) histogram : ");

        if(type == 1) {
            System.out.println("------------TP-------------------");
            for (int tpidx = 0; tpidx < 4; tpidx++) {
                if(tpidx==2) continue;
                System.out.printf("TP Transaction %2d : max rt : %10.2f | min rt : %10.2f | avg rt : %10.2f | 95%% rt : %10.2f | 99%% rt : %10.2f \n",
                        (tpidx + 1),
                        hist.getTPItem(tpidx).getMax(),
                        hist.getTPItem(tpidx).getMin(),
                        hist.getTPItem(tpidx).getMean(),
                        hist.getTPItem(tpidx).getPercentile(95),
                        hist.getTPItem(tpidx).getPercentile(99));
            }

            logger.info("-----------Avg-Lag-Time--------------------");
            logger.info("Lag Time (ms) : "+ getLagtime() * 1.0);
            System.out.println("-----------C-Score--------------------");
            System.out.printf("C-Score : %10.2f \n", (getLagtime() / node_num)  * 1.0);
        }

        if(type == 2) {
            System.out.println("------------TP-------------------");
            for (int tpidx = 0; tpidx < 3; tpidx++) {
                System.out.printf("TP Transaction %2d : max rt : %10.2f | min rt : %10.2f | avg rt : %10.2f | 95%% rt : %10.2f | 99%% rt : %10.2f \n",
                        (tpidx + 1),
                        hist.getTPItem(tpidx).getMax(),
                        hist.getTPItem(tpidx).getMin(),
                        hist.getTPItem(tpidx).getMean(),
                        hist.getTPItem(tpidx).getPercentile(95),
                        hist.getTPItem(tpidx).getPercentile(99));
            }
            System.out.println("-----------P-Score--------------------");
            System.out.printf("P-Score : %10.2f \n", (getTps() / (rcu_c*cpu_num+rcu_m*mem_num)*node_num)  * 1.0);
        }

        if(type==8){
            for (int i = 0; i < tenant_num; i++) {
                logger.info("Tenant"+(i+1)+" : final tps is " + tpsList[i]);
            }
        }

        if(type==9){
            for (int i = 0; i < tenant_num; i++) {
                logger.info("Tenant"+(i+1)+" : final aps is " + apsList[i]);
            }
        }

        logger.info("====================Thank you!========================");
    }
}
