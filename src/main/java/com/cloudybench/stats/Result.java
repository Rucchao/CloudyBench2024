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
    private long tpTotal;
    private long F_Score_RW;
    private long R_Score_RW;
    private long F_Score_RO;
    private long R_Score_RO;
    private long F_Score;
    private long R_Score;
    private double geometric_tps;
    private double tps;
    private double tps_rw;
    private double tps_ro;
    private double elastic_tps;
    private double T_Score;
    private double P_SCORE;
    private double E1_SCORE;
    private double E2_SCORE;
    private String startTS ;
    private String endTs;
    private Histogram hist;
    private double freshness = 0;
    private ArrayList<Long> laglist;
    private long lagtime = 0;
    private long C_Score = 0;
    private int apclient ;
    private int tpclient;
    private int xapclient ;
    private int xtpclient;



    public Result(int tenant_num){
        this.tenant_num=tenant_num;
    }

    public void setTenant_num(int num) {
        this.tenant_num = num;
    }

    public void setElastic_tps(double tps) {
        this.elastic_tps = elastic_tps;
    }

    public double getElastic_tps() {
        return this.elastic_tps;
    }

    public void setE1_SCORE(double score) {
        this.E1_SCORE = score;
    }

    public double getE1_SCORE() {
        return this.E1_SCORE;
    }

    public void setE2_SCORE(double score) {
        this.E2_SCORE = score;
    }

    public double getE2_SCORE() {
        return this.E2_SCORE;
    }

    public void setP_SCORE(double score) {
        this.P_SCORE = score;
    }

    public double getP_SCORE() {
        return this.P_SCORE;
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

    public void setDbType(String dbType) {
        this.dbType = dbType;
    }

    public void setEndTs(String endTs) {
        this.endTs = endTs;
    }

    public void setStartTS(String startTS) {
        this.startTS = startTS;
    }

    public void setTps(double tps) {
        this.tps = tps;
    }

    public void setF_Score_RW(long f_score) {
        this.F_Score_RW = f_score;
    }

    public long getF_Score_RW(){
        return this.F_Score_RW;
    }

    public void setF_Score(long f_score) {
        this.F_Score = f_score;
    }

    public long getF_Score(){
        return this.F_Score;
    }

    public void setR_Score(long r_score) {
        this.R_Score = r_score;
    }

    public long getR_Score(){
        return this.R_Score;
    }

    public void setT_Score(double t_score) {
        this.T_Score = t_score;
    }

    public double getT_Score(){
        return this.T_Score;
    }

    public void setR_Score_RW(long r_score) {
        this.R_Score_RW = r_score;
    }

    public long getR_Score_RW(){ return this.R_Score_RW; }

    public void setF_Score_RO(long f_score) {
        this.F_Score_RO = f_score;
    }

    public long getF_Score_RO(){
        return this.F_Score_RO;
    }

    public void setR_Score_RO(long r_score) {
        this.R_Score_RO = r_score;
    }

    public long getR_Score_RO(){ return this.R_Score_RO; }

    public void setTps_geometric(double tps) {
        this.geometric_tps = tps;
    }

    public void setTps_rw(double tps) {
        this.tps_rw = tps;
    }

    public void setTps_ro(double tps) {
        this.tps_ro = tps;
    }

    public double getTps_rw(){
        return this.tps_rw;
    }

    public double getTps_ro(){
        return this.tps_ro;
    }

    public double getTps_geometric(){
        return this.geometric_tps;
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

    public void setLagtime(long time) { this.lagtime=time; }

    public void setC_Score(long c_score) {
        this.C_Score = c_score;
    }

    public double getC_Score(){
        return this.C_Score;
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

    public double getTps() {
        return tps;
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

    public void printResult(int type){
        logger.info("====================Test Summary========================");
        logger.info("Test starts at " + getStartTS());
        logger.info("Test ends at " + getEndTs());
        Double rcu_c = Double.parseDouble(ConfigLoader.prop.getProperty("rcu_c","1"));
        Double rcu_m = Double.parseDouble(ConfigLoader.prop.getProperty("rcu_m","1"));
        Double rcu_io = Double.parseDouble(ConfigLoader.prop.getProperty("rcu_io","1"));
        Double rcu_mbps = Double.parseDouble(ConfigLoader.prop.getProperty("rcu_mbps","1"));
        int cpu_num = Integer.parseInt(ConfigLoader.prop.getProperty("cpu_num","1"));
        int mem_num = Integer.parseInt(ConfigLoader.prop.getProperty("mem_num","1"));
        int IOPS = Integer.parseInt(ConfigLoader.prop.getProperty("IOPS","1"));
        int Network = Integer.parseInt(ConfigLoader.prop.getProperty("Network","1"));
        int node_num = Integer.parseInt(ConfigLoader.prop.getProperty("node_num","1"));

        switch(type){
            case 1:
            case 2:
            case 4:
                logger.info("TP Concurrency is " + getTpclient());
                logger.info("Total amount of TP Transaction is " + getTpTotal());
                logger.info("TPS is " + getTps());
                break;
            case 3:
                logger.info("TP Concurrency is " + getTpclient());
                logger.info("Total amount of TP Transaction is " + getTpTotal());
                logger.info("TPS is " + getTps_rw());
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
            long lagtime = getLagtime();
            setLagtime(lagtime);
            logger.info("Lag Time (ms) : "+ lagtime);
            System.out.println("-----------C-Score--------------------");
            long C_Score= lagtime / node_num;
            setC_Score(C_Score);
            System.out.printf("C-Score : %d \n", C_Score);
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
            double P_Score = (getTps() / (rcu_c*cpu_num+rcu_m*mem_num+rcu_io*IOPS+rcu_mbps*Network)*node_num)  * 1.0;
            setP_SCORE(P_Score);
            System.out.printf("P-Score : %10.5f \n", P_Score);
        }

        if(type == 3) {
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
        }

        if(type == 4 || type ==5 ) {
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
        }

        if(type==8){
            for (int i = 0; i < tenant_num; i++) {
                logger.info("Tenant"+(i+1)+" : final tps is " + tpsList[i]);
            }
        }
        //logger.info("====================Thank you!========================");
    }
}
