package com.hybench.stats;

import com.hybench.HyBench;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
    private long atTotal;
    private long iqTotal;
    private double tps;
    private double qps;
    private double xptps;
    private double xpqps;
    private double atps;
    private double iqps;
    private String startTS ;
    private String endTs;
    private Histogram hist;
    private double freshness = 0;
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

    public void setTpsList(double[] TpsList) {
        this.tpsList = TpsList;
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

    public void setFresh(double freshness){
        this.freshness = freshness;
    }

    public void printResult(int type){
        logger.info("====================Test Summary========================");
        logger.info("Test starts at " + getStartTS());
        logger.info("Test ends at " + getEndTs());
	    logger.info("Risk Rate is " + getRiskRate());
        switch(type){
            case 0:
                logger.info("XP-IQ Concurrency is " + getXapclient());
                logger.info("XP-AT Concurrency is " + getXtpclient());
                logger.info("Total amount of XP-IQ Queries is " + getIqTotal());
                logger.info("Total amount of XP-AT Transaction is " + getAtTotal());
                logger.info("XP-QPS is " + getXpqps());
                logger.info("XP-TPS is " + getXptps());
                break;
            case 1:
                logger.info("AP Concurrency is " + getApclient());
                logger.info("TP Concurrency is " + getTpclient());
                logger.info("Total amount of TP Transaction is " + getTpTotal());
                logger.info("TPS is " + getTps());
                break;
            case 7 :
                logger.info("AP Concurrency is " + getApclient());
                logger.info("TP Concurrency is " + getTpclient());
                logger.info("Total amount of AP Queries is " + getApTotal());
                logger.info("QPS is " + getQps());
                break;
            case 3:
                logger.info("AP Concurrency is " + getApclient());
                logger.info("TP Concurrency is " + getTpclient());
                logger.info("XP-IQ Concurrency is " + getXapclient());
                logger.info("XP-AT Concurrency is " + getXtpclient());
                logger.info("Total amount of TP Transaction is " + getTpTotal());
                logger.info("TPS is " + getTps());
                logger.info("Total amount of AP Queries is " + getApTotal());
                logger.info("QPS is " + getQps());
                logger.info("Total amount of XP-IQ Queries is " + getIqTotal());
                logger.info("Total amount of XP-AT Transaction is " + getAtTotal());
                logger.info("XP-QPS is " + getXpqps());
                logger.info("XP-TPS is " + getXptps());
                break;
            case 4:
                logger.info("XP-IQ Concurrency is " + getXapclient());
                logger.info("XP-AT Concurrency is " + getXtpclient());
                logger.info("Total amount of XP-IQ Queries is " + getIqTotal());
                logger.info("Total amount of XP-AT Transaction is " + getAtTotal());
                logger.info("Fresh-XP-QPS is " + getXpqps());
                logger.info("Fresh-XP-TPS is " + getXptps());
                break;
            case 6:
                logger.info("AP Concurrency is " + getApclient());
                logger.info("TP Concurrency is " + getTpclient());
                logger.info("Total amount of TP Transaction is " + getTpTotal());
                logger.info("TPS is " + getTps());
                logger.info("Total amount of AP Queries is " + getApTotal());
                logger.info("QPS is " + getQps());
                logger.info("XP-IQ Concurrency is " + getXapclient());
                logger.info("XP-AT Concurrency is " + getXtpclient());
                logger.info("Total amount of XP-IQ Queries is " + getIqTotal());
                logger.info("Total amount of XP-AT Transaction is " + getAtTotal());
                logger.info("Fresh-XP-QPS is " + getXpqps());
                logger.info("Fresh-XP-TPS is " + getXptps());
                break;

        }
        logger.info("Query/Transaction response time(ms) histogram : ");
        if( type == 2 || type == 6 || type == 7) {
            System.out.println("------------AP-------------------");
            for (int apidx = 0; apidx < 13; apidx++) {
                System.out.printf("AP Query %2d : max rt : %10.2f | min rt : %10.2f | avg rt : %10.2f | 95%% rt : %10.2f | 99%% rt : %10.2f \n",
                        (apidx + 1),
                        hist.getAPItem(apidx).getMax(),
                        hist.getAPItem(apidx).getMin(),
                        hist.getAPItem(apidx).getMean(),
                        hist.getAPItem(apidx).getPercentile(95),
                        hist.getAPItem(apidx).getPercentile(99));
            }
        }

        if(type == 1 || type == 6) {
            System.out.println("------------TP-------------------");
            for (int tpidx = 0; tpidx < 18; tpidx++) {
                System.out.printf("TP Transaction %2d : max rt : %10.2f | min rt : %10.2f | avg rt : %10.2f | 95%% rt : %10.2f | 99%% rt : %10.2f \n",
                        (tpidx + 1),
                        hist.getTPItem(tpidx).getMax(),
                        hist.getTPItem(tpidx).getMin(),
                        hist.getTPItem(tpidx).getMean(),
                        hist.getTPItem(tpidx).getPercentile(95),
                        hist.getTPItem(tpidx).getPercentile(99));
            }
        }

        if(type == 0 || type == 4 || type == 6){
            System.out.println("-----------XP-IQ--------------------");
            for (int xpidx = 0; xpidx < 6; xpidx++) {
                System.out.printf("Interative Query %d : max rt : %10.2f | min rt : %10.2f | avg rt : %10.2f | 95%% rt : %10.2f | 99%% rt : %10.2f \n",
                        (xpidx + 1),
                        hist.getXPIQItem(xpidx).getMax(),
                        hist.getXPIQItem(xpidx).getMin(),
                        hist.getXPIQItem(xpidx).getMean(),
                        hist.getXPIQItem(xpidx).getPercentile(95),
                        hist.getXPIQItem(xpidx).getPercentile(99));
            }
        }

        if(type == 0 || type == 4 || type == 6) {
            System.out.println("-----------XP-AT--------------------");
            for (int tpidx = 0; tpidx < 6; tpidx++) {
                System.out.printf("Analytical Transaction AT%d : max rt : %10.2f | min rt : %10.2f | avg rt : %10.2f | 95%% rt : %10.2f | 99%% rt : %10.2f \n",
                        (tpidx + 1),
                        hist.getXPATItem(tpidx).getMax(),
                        hist.getXPATItem(tpidx).getMin(),
                        hist.getXPATItem(tpidx).getMean(),
                        hist.getXPATItem(tpidx).getPercentile(95),
                        hist.getXPATItem(tpidx).getPercentile(99));
            }
        }

        if( type == 3 ) {
            logger.info("-----------HTAP-Summary--------------------");
            logger.info("-----------AP-Part--------------------");
            logger.info("QPS : "+qps );
            logger.info("-----------TP-Part--------------------");
            logger.info("TPS : "+tps );
            logger.info("-----------XP-Part--------------------");
            logger.info("XP-QPS : "+xpqps );
            logger.info("XP-TPS : "+xptps );
            logger.info("-----------HTAP-Score--------------------");
            logger.info("Geometric Mean : "+Math.pow(qps*tps*(xpqps+xptps), 1/3.0));

        }

        if( type == 6 ) {
            logger.info("-----------HTAP-Summary--------------------");
            logger.info("-----------AP-Part--------------------");
            logger.info("QPS : "+qps );
            logger.info("-----------TP-Part--------------------");
            logger.info("TPS : "+tps );
            logger.info("-----------XP-Part--------------------");
            logger.info("XP-QPS : "+xpqps );
            logger.info("XP-TPS : "+xptps );
            logger.info("-----------Avg-Freshness-Score--------------------");
            logger.info("Freshness(ms) : "+getFresh() * 1.0);
            logger.info("-----------HTAP-Score--------------------");
            logger.info("Geometric Mean : "+Math.pow(qps*tps*(xpqps+xptps), 1/3.0)/(1+getFresh()/1000));
        }

        if(type == 4){
            logger.info("-----------Avg-Freshness-Score--------------------");
            logger.info("Freshness(ms) : "+ getFresh() * 1.0);
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


    public double getXptps() {
        return xptps;
    }

    public void setXpqps(double xpqps) {
        this.xpqps = xpqps;
    }

    public double getXpqps() {
        return xpqps;
    }

    public void setXptps(double xptps) {
        this.xptps = xptps;
    }

    public double getAtps() {
        return atps;
    }

    public void setIqps(double iqps) {
        this.iqps = iqps;
    }

    public double getIqps() {
        return iqps;
    }

    public void setAtps(double atps) {
        this.atps = atps;
    }

    public long getAtTotal() {
        return atTotal;
    }

    public void setAtTotal(long atTotal) {
        this.atTotal = atTotal;
    }

    public long getIqTotal() {
        return iqTotal;
    }

    public void setIqTotal(long iqTotal) {
        this.iqTotal = iqTotal;
    }
}
