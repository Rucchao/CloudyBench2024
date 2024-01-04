package com.hybench.workload;
/**
 *
 * @time 2023-03-04
 * @version 1.0.0
 * @file APClient.java
 * @description
 *   AP Client Processor
 **/


import com.hybench.dbconn.ConnectionMgr;
import com.hybench.util.RandomGenerator;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.*;
import java.util.ArrayList;

public class CloudAPClient extends Client{
    int customer_no;
    int company_no;
//    RandomGenerator rg = new RandomGenerator();

    @Override
    // init the db here
    public void doInit(){

        // tentative parameter curation
        Long customer_number = CR.customer_number;
        Long company_number= CR.company_number;
        customer_no = customer_number.intValue() + 1;
        company_no = company_number.intValue();

    }

    public int Get_blocked_transfer_Id(){
        int Id=0;
        Id=Related_Blocked_Transfer_ids.get(rg.getRandomint(Related_Blocked_Transfer_ids.size()));
        return Id;
    }

    public int Get_blocked_account_Id(){
        if(queue_ids.size()==0)
            return rg.getRandomint(customer_no, customer_no+company_no);
        else{
            int Id=0;
            int index = rg.getRandomint(queue_ids.size());
            int i = 0;
            for(Integer obj : queue_ids)
            {
                if (i == index)
                    Id=obj;
                i++;
            }
            return Id;
        }
    }


    // 6 Interative Queries
    public ClientResult execIQ1(Connection conn){
        ClientResult cr = new ClientResult();
        PreparedStatement pstmt = null;
        int custid;
        long responseTime = 0L;
        try {
            pstmt = conn.prepareStatement(sqls.ap_iq1());

            double rate= rg.getRandomDouble();
            if(rate<risk_rate/2)
                custid = Get_blocked_transfer_Id();
            else if (rate<risk_rate){
                custid = Get_blocked_account_Id();
            }
            else
                custid=rg.getRandomint(customer_no);

            pstmt.setInt(1,custid);
            pstmt.setInt(2,custid);
            long currentStarttTs = System.currentTimeMillis();
            pstmt.execute();
            long currentEndTs = System.currentTimeMillis();
            responseTime = currentEndTs - currentStarttTs;
            logger.debug(pstmt.toString());
            hist.getXPIQItem(0).addValue(responseTime);
            lock.lock();
            iqTotalCount++;
            lock.unlock();
        } catch (SQLException e) {
            e.printStackTrace();
            cr.setResult(false);
            cr.setErrorMsg(e.getMessage());
            cr.setErrorCode(String.valueOf(e.getErrorCode()));
        }  finally {
            cr.setRt(responseTime);
            try {
                pstmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return cr;
    }

    public ClientResult execIQ2(Connection conn){
        ClientResult cr = new ClientResult();
        PreparedStatement pstmt = null;
        int companyid;
        long responseTime = 0L;
        try {
            pstmt = conn.prepareStatement(sqls.ap_iq2());
            //  companyid = rg.getRandomint(customer_no, customer_no+company_no);
            double rate= rg.getRandomDouble();
            if(rate<risk_rate)
                companyid = Get_blocked_account_Id();
            else
                companyid = rg.getRandomint(customer_no, customer_no+company_no);


            pstmt.setInt(1,companyid);
            pstmt.setInt(2,companyid);
            long currentStarttTs = System.currentTimeMillis();
            pstmt.execute();
            long currentEndTs = System.currentTimeMillis();
            responseTime = currentEndTs - currentStarttTs;
            logger.debug(pstmt.toString());
            hist.getXPIQItem(1).addValue(responseTime);
            lock.lock();
            apTotalCount++;
            lock.unlock();
        } catch (SQLException e) {
            e.printStackTrace();
            cr.setResult(false);
            cr.setErrorMsg(e.getMessage());
            cr.setErrorCode(String.valueOf(e.getErrorCode()));
        }  finally {
            cr.setRt(responseTime);
            try {
                pstmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return cr;
    }

    public ClientResult execIQ3(Connection conn){
        ClientResult cr = new ClientResult();
        PreparedStatement pstmt = null;
        int companyid=0;
        long responseTime = 0L;
        try {
            pstmt = conn.prepareStatement(sqls.ap_iq3());
            companyid = rg.getRandomint(customer_no,customer_no+company_no);
            pstmt.setInt(1,companyid);
            long currentStarttTs = System.currentTimeMillis();
            pstmt.execute();
            long currentEndTs = System.currentTimeMillis();
            responseTime = currentEndTs - currentStarttTs;
            logger.debug(pstmt.toString());
            hist.getXPIQItem(2).addValue(responseTime);
            lock.lock();
            iqTotalCount++;
            lock.unlock();
        } catch (SQLException e) {
            e.printStackTrace();
            cr.setResult(false);
            cr.setErrorMsg(e.getMessage());
            cr.setErrorCode(String.valueOf(e.getErrorCode()));
        } finally {
            cr.setRt(responseTime);
            try {
                pstmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return cr;
    }

    public ClientResult execIQ4(Connection conn){
        ClientResult cr = new ClientResult();
        PreparedStatement pstmt = null;
        int companyid;
        long responseTime = 0L;
        try {
            pstmt = conn.prepareStatement(sqls.ap_iq4());
            companyid = rg.getRandomint(customer_no,customer_no+company_no);
            pstmt.setInt(1,companyid);
            pstmt.setInt(2,companyid);
            long currentStarttTs = System.currentTimeMillis();
            pstmt.execute();
            long currentEndTs = System.currentTimeMillis();
            responseTime = currentEndTs - currentStarttTs;
            hist.getXPIQItem(3).addValue(responseTime);
            logger.debug(pstmt.toString());
            lock.lock();
            iqTotalCount++;
            lock.unlock();
        } catch (SQLException e) {
            e.printStackTrace();
            cr.setResult(false);
            cr.setErrorMsg(e.getMessage());
            cr.setErrorCode(String.valueOf(e.getErrorCode()));
        }  finally {
            cr.setRt(responseTime);
            try {
                pstmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return cr;
    }

    public ClientResult execIQ5(Connection conn){

        ClientResult cr = new ClientResult();
        Statement stmt = null;
        int applicantid=1;
        try{
            stmt = conn.createStatement();;
            ResultSet rs = stmt.executeQuery(sqls.ap_iq5_1());
            while ( rs.next() ) {
                applicantid= rs.getInt(1);
            }
            rs.close();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }finally {

            try {
                stmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        PreparedStatement pstmt = null;
        long responseTime = 0L;
        try {
            pstmt = conn.prepareStatement(sqls.ap_iq5());
            pstmt.setInt(1,applicantid);
            long currentStarttTs = System.currentTimeMillis();
            pstmt.execute();
            long currentEndTs = System.currentTimeMillis();
            responseTime = currentEndTs - currentStarttTs;
            hist.getXPIQItem(4).addValue(responseTime);
            logger.debug(pstmt.toString());
            lock.lock();
            iqTotalCount++;
            lock.unlock();
        } catch (SQLException e) {
            e.printStackTrace();
            cr.setResult(false);
            cr.setErrorMsg(e.getMessage());
            cr.setErrorCode(String.valueOf(e.getErrorCode()));
        }  finally {
            cr.setRt(responseTime);
            try {
                pstmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return cr;
    }

    public ClientResult execIQ6(Connection conn){

        ClientResult cr = new ClientResult();
        PreparedStatement pstmt = null;
        long responseTime = 0L;
        try {
            pstmt = conn.prepareStatement(sqls.ap_iq6());
            long currentStarttTs = System.currentTimeMillis();
            pstmt.execute();
            long currentEndTs = System.currentTimeMillis();
            responseTime = currentEndTs - currentStarttTs;
            hist.getXPIQItem(5).addValue(responseTime);
            logger.debug(pstmt.toString());
            lock.lock();
            iqTotalCount++;
            lock.unlock();
        } catch (SQLException e) {
            e.printStackTrace();
            cr.setResult(false);
            cr.setErrorMsg(e.getMessage());
            cr.setErrorCode(String.valueOf(e.getErrorCode()));
        }  finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return cr;
    }

    // 12 AP Queries
    public ClientResult execQ1(Connection conn){
        ClientResult cr = new ClientResult();
        PreparedStatement pstmt = null;
        int custid;
        long responseTime = 0L;
        try {
            pstmt = conn.prepareStatement(sqls.ap_q1());
            custid = rg.getRandomint(customer_no);
            pstmt.setInt(1,custid);
            pstmt.setInt(2,custid);
            pstmt.setInt(3,custid);
            pstmt.setInt(4,custid);
            long currentStarttTs = System.currentTimeMillis();
            pstmt.execute();
            long currentEndTs = System.currentTimeMillis();
            logger.debug(pstmt.toString());
            responseTime = currentEndTs - currentStarttTs;
            hist.getAPItem(0).addValue(responseTime);
            lock.lock();
            apTotalList[tenant_num-1]++;
            lock.unlock();
        } catch (SQLException e) {
            e.printStackTrace();
            cr.setResult(false);
            cr.setErrorMsg(e.getMessage());
            cr.setErrorCode(String.valueOf(e.getErrorCode()));
        }  finally {
            cr.setRt(responseTime);
            try {
                pstmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return cr;
    }

    public ClientResult execQ2(Connection conn){
        ClientResult cr = new ClientResult();
        PreparedStatement pstmt = null;
        PreparedStatement stmt = null;
        int custid = 1;
        try{
            stmt = conn.prepareStatement(sqls.ap_q2_1());
            stmt.setInt(1,customer_no);
            ResultSet rs = stmt.executeQuery();
            if ( rs.next() ) {
                custid= rs.getInt(1);
            }
            rs.close();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }finally {
            try {
                stmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        long responseTime = 0L;
        try {
            pstmt = conn.prepareStatement(sqls.ap_q2());
            pstmt.setInt(1,custid);
            long currentStarttTs = System.currentTimeMillis();
            pstmt.execute();
            long currentEndTs = System.currentTimeMillis();
            responseTime = currentEndTs - currentStarttTs;
            hist.getAPItem(1).addValue(responseTime);
            logger.debug(pstmt.toString());
            lock.lock();
            apTotalList[tenant_num-1]++;
            lock.unlock();
        } catch (SQLException e) {
            e.printStackTrace();
            cr.setResult(false);
            cr.setErrorMsg(e.getMessage());
            cr.setErrorCode(String.valueOf(e.getErrorCode()));
        }  finally {
            cr.setRt(responseTime);
            try {
                pstmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return cr;
    }

    public ClientResult execQ3(Connection conn){
        ClientResult cr = new ClientResult();
        PreparedStatement pstmt = null;
        int custid;
        long responseTime = 0L;
        try {
            pstmt = conn.prepareStatement(sqls.ap_q3());
            custid = rg.getRandomint(customer_no);
            pstmt.setInt(1,custid);
            pstmt.setInt(2,custid);
            long currentStarttTs = System.currentTimeMillis();
            pstmt.execute();
            long currentEndTs = System.currentTimeMillis();
            responseTime = currentEndTs - currentStarttTs;
            logger.debug(pstmt.toString());
            hist.getAPItem(2).addValue(responseTime);
            lock.lock();
            apTotalList[tenant_num-1]++;
            lock.unlock();
        } catch (SQLException e) {
            e.printStackTrace();
            cr.setResult(false);
            cr.setErrorMsg(e.getMessage());
            cr.setErrorCode(String.valueOf(e.getErrorCode()));
        }  finally {
            cr.setRt(responseTime);
            try {
                pstmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return cr;
    }

    public ClientResult execQ4(Connection conn){
        ClientResult cr = new ClientResult();
        PreparedStatement pstmt = null;
        int custid;
        long responseTime = 0L;
        try {
            pstmt = conn.prepareStatement(sqls.ap_q4());
            custid = rg.getRandomint(customer_no);
            pstmt.setInt(1,custid);
            pstmt.setInt(2,custid);
            long currentStarttTs = System.currentTimeMillis();
            pstmt.execute();
            long currentEndTs = System.currentTimeMillis();
            responseTime = currentEndTs - currentStarttTs;
            logger.debug(pstmt.toString());
            hist.getAPItem(3).addValue(responseTime);
            lock.lock();
            apTotalList[tenant_num-1]++;
            lock.unlock();
        } catch (SQLException e) {
            e.printStackTrace();
            cr.setResult(false);
            cr.setErrorMsg(e.getMessage());
            cr.setErrorCode(String.valueOf(e.getErrorCode()));
        }  finally {
            cr.setRt(responseTime);
            try {
                pstmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return cr;
    }

    public ClientResult execQ5(Connection conn){
        ClientResult cr = new ClientResult();
        PreparedStatement pstmt = null;
        int companyid;
        long responseTime = 0L;
        try {
            pstmt = conn.prepareStatement(sqls.ap_q5());
            companyid = rg.getRandomint(customer_no,customer_no+company_no);
            pstmt.setInt(1,companyid);

            long currentStarttTs = System.currentTimeMillis();
            pstmt.execute();
            long currentEndTs = System.currentTimeMillis();
            responseTime = currentEndTs - currentStarttTs;
            logger.debug(pstmt.toString());
            hist.getAPItem(4).addValue(responseTime);
            lock.lock();
            apTotalList[tenant_num-1]++;
            lock.unlock();
        } catch (SQLException e) {
            e.printStackTrace();
            cr.setResult(false);
            cr.setErrorMsg(e.getMessage());
            cr.setErrorCode(String.valueOf(e.getErrorCode()));
        }  finally {
            cr.setRt(responseTime);
            try {
                pstmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return cr;
    }

    public ClientResult execQ6(Connection conn){
        ClientResult cr = new ClientResult();
        PreparedStatement pstmt = null;
        long responseTime = 0L;
        try {
            pstmt = conn.prepareStatement(sqls.ap_q6());
            long currentStarttTs = System.currentTimeMillis();
            pstmt.execute();
            long currentEndTs = System.currentTimeMillis();
            responseTime = currentEndTs - currentStarttTs;
            logger.debug(pstmt.toString());
            hist.getAPItem(5).addValue(responseTime);
            lock.lock();
            apTotalList[tenant_num-1]++;
            lock.unlock();
        } catch (SQLException e) {
            e.printStackTrace();
            cr.setResult(false);
            cr.setErrorMsg(e.getMessage());
            cr.setErrorCode(String.valueOf(e.getErrorCode()));
        }  finally {
            cr.setRt(responseTime);
            try {
                pstmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return cr;
    }

    public ClientResult execQ7(Connection conn){
        ClientResult cr = new ClientResult();
        PreparedStatement pstmt = null;
        int companyid;
        long responseTime = 0L;
        try {
            pstmt = conn.prepareStatement(sqls.ap_q7());
            //int cid = rg.getRandomint(10000);
            companyid = rg.getRandomint(customer_no,customer_no+company_no);
            pstmt.setInt(1,companyid);
            pstmt.setInt(2,companyid);
            pstmt.setInt(3,companyid);
            pstmt.setInt(4,companyid);

            long currentStarttTs = System.currentTimeMillis();
            pstmt.execute();
            long currentEndTs = System.currentTimeMillis();
            responseTime = currentEndTs - currentStarttTs;
            logger.debug(pstmt.toString());
            hist.getAPItem(6).addValue(responseTime);
            lock.lock();
            apTotalList[tenant_num-1]++;
            lock.unlock();
        } catch (SQLException e) {
            e.printStackTrace();
            cr.setResult(false);
            cr.setErrorMsg(e.getMessage());
            cr.setErrorCode(String.valueOf(e.getErrorCode()));
        }  finally {
            cr.setRt(responseTime);
            try {
                pstmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return cr;
    }

    public ClientResult execQ8(Connection conn){
        ClientResult cr = new ClientResult();
        PreparedStatement pstmt = null;
        long responseTime = 0L;
        try {
            pstmt = conn.prepareStatement(sqls.ap_q8());
            long currentStarttTs = System.currentTimeMillis();
            pstmt.execute();
            long currentEndTs = System.currentTimeMillis();
            responseTime = currentEndTs - currentStarttTs;
            logger.debug(pstmt.toString());
            hist.getAPItem(7).addValue(responseTime);
            lock.lock();
            apTotalList[tenant_num-1]++;
            lock.unlock();
        } catch (SQLException e) {
            e.printStackTrace();
            cr.setResult(false);
            cr.setErrorMsg(e.getMessage());
            cr.setErrorCode(String.valueOf(e.getErrorCode()));
        }  finally {
            cr.setRt(responseTime);
            try {
                pstmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return cr;
    }

    public ClientResult execQ9(Connection conn){
        ClientResult cr = new ClientResult();
        PreparedStatement pstmt = null;
        long responseTime = 0L;
        try {
            pstmt = conn.prepareStatement(sqls.ap_q9());
            long currentStarttTs = System.currentTimeMillis();
            pstmt.execute();
            long currentEndTs = System.currentTimeMillis();
            responseTime = currentEndTs - currentStarttTs;
            logger.debug(pstmt.toString());
            hist.getAPItem(8).addValue(responseTime);
            lock.lock();
            apTotalList[tenant_num-1]++;
            lock.unlock();
        } catch (SQLException e) {
            e.printStackTrace();
            cr.setResult(false);
            cr.setErrorMsg(e.getMessage());
            cr.setErrorCode(String.valueOf(e.getErrorCode()));
        }  finally {
            cr.setRt(responseTime);
            try {
                pstmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return cr;
    }

    public ClientResult execQ10(Connection conn){
        ClientResult cr = new ClientResult();
        PreparedStatement pstmt = null;
        long responseTime = 0L;
        try {
            pstmt = conn.prepareStatement(sqls.ap_q10());
            long currentStarttTs = System.currentTimeMillis();
            pstmt.execute();
            long currentEndTs = System.currentTimeMillis();
            responseTime = currentEndTs - currentStarttTs;
            logger.debug(pstmt.toString());
            hist.getAPItem(9).addValue(responseTime);
            lock.lock();
            apTotalList[tenant_num-1]++;
            lock.unlock();
        } catch (SQLException e) {
            e.printStackTrace();
            cr.setResult(false);
            cr.setErrorMsg(e.getMessage());
            cr.setErrorCode(String.valueOf(e.getErrorCode()));
        }  finally {
            cr.setRt(responseTime);
            try {
                pstmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return cr;
    }

    public ClientResult execQ11(Connection conn){
        ClientResult cr = new ClientResult();
        PreparedStatement pstmt = null;
        long responseTime = 0L;
        try {
            pstmt = conn.prepareStatement(sqls.ap_q11());
            long currentStarttTs = System.currentTimeMillis();
            pstmt.execute();
            long currentEndTs = System.currentTimeMillis();
            responseTime = currentEndTs - currentStarttTs;
            logger.debug(pstmt.toString());
            hist.getAPItem(10).addValue(responseTime);
            lock.lock();
            apTotalList[tenant_num-1]++;
            lock.unlock();
        } catch (SQLException e) {
            e.printStackTrace();
            cr.setResult(false);
            cr.setErrorMsg(e.getMessage());
            cr.setErrorCode(String.valueOf(e.getErrorCode()));
        }  finally {
            cr.setRt(responseTime);
            try {
                pstmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return cr;
    }

    public ClientResult execQ12(Connection conn){
        ClientResult cr = new ClientResult();
        String Category = rg.getRandomCategory();
        PreparedStatement pstmt = null;
        long responseTime = 0L;
        try {
            pstmt = conn.prepareStatement(sqls.ap_q12());
            long currentStarttTs = System.currentTimeMillis();
            pstmt.setString(1, Category);
            pstmt.execute();
            long currentEndTs = System.currentTimeMillis();
            logger.debug(pstmt.toString());
            responseTime = currentEndTs - currentStarttTs;
            hist.getAPItem(11).addValue(responseTime);
            lock.lock();
            apTotalList[tenant_num-1]++;
            lock.unlock();
        } catch (SQLException e) {
            e.printStackTrace();
            cr.setResult(false);
            cr.setErrorMsg(e.getMessage());
            cr.setErrorCode(String.valueOf(e.getErrorCode()));
        }  finally {
            cr.setRt(responseTime);
            try {
                pstmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return cr;
    }

    public ClientResult execQ13(Connection conn){
        ClientResult cr = new ClientResult();
        PreparedStatement pstmt = null;
        int custid;
        long responseTime = 0L;
        try {
            pstmt = conn.prepareStatement(sqls.ap_q13());
            custid = rg.getRandomint(customer_no);
            long currentStarttTs = System.currentTimeMillis();
            pstmt.setInt(1,custid);
            pstmt.execute();
            long currentEndTs = System.currentTimeMillis();
            responseTime = currentEndTs - currentStarttTs;
            logger.debug(pstmt.toString());
            hist.getAPItem(12).addValue(responseTime);
            lock.lock();
            apTotalList[tenant_num-1]++;
            lock.unlock();
        } catch (SQLException e) {
            e.printStackTrace();
            cr.setResult(false);
            cr.setErrorMsg(e.getMessage());
            cr.setErrorCode(String.valueOf(e.getErrorCode()));
        }  finally {
            cr.setRt(responseTime);
            try {
                pstmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return cr;
    }

    @Override
    public ClientResult execute() {
        int type = getTaskType();
        // get the tenant url
        logger.info("Current AP Client Tenant Number is "+tenant_num);

        Connection conn = ConnectionMgr.getConnection(tenant_num,true);

        ClientResult ret = new ClientResult();
        long totalElapsedTime = 0L;
        try {
            Class<CloudAPClient> apClass = (Class<CloudAPClient>)Class.forName("com.hybench.workload.CloudAPClient");
            if(type == 9){
                int round = 1;
                while(!exitFlag){
                    long roundElapsedTime = 0L;
                    ArrayList<Integer> runList = getRandomList(1,13);
                    StringBuilder sb = new StringBuilder("Tenant"+tenant_num+" : Current thread " + Thread.currentThread().getName() + " - Run List in current round "+ round+" : ");
                    for(int idx:runList){
                        sb.append("Q").append(idx).append(" ");
                    }
                    logger.info(sb.toString());
                    int i = 0;
                    for ( i = 0; i < 13; i++) {
                        Method method = apClass.getMethod("execQ" + runList.get(i), Connection.class);
                        ClientResult cr = (ClientResult) method.invoke(this, conn);
                        totalElapsedTime += cr.getRt();
                        roundElapsedTime += cr.getRt();
                    }
                    if (exitFlag) {
                        break;
                    }
                    logger.info("Tenant "+tenant_num+" : APClient Current thread " + Thread.currentThread().getName() + " - round " + round + " elapsed time is " + String.format("%d",roundElapsedTime) + "(ms)");
                    //logger.info("Tenant "+tenant_num+" : APClient Current thread " + Thread.currentThread().getName() + " - round " + round + " is done");
                    round++;
                }
                ret.setApRound(round);
                ret.setRt(totalElapsedTime);
            }
            else if(type== 0 || type == 4) {
                while(!exitFlag){
                    int i = rg.getRandomint(1, 7);
                    Method method = apClass.getMethod("execIQ" + i,Connection.class);
                    ClientResult cr = (ClientResult) method.invoke(this, conn);
                    totalElapsedTime += cr.getRt();
                    if(exitFlag)
                        break;
                }
                ret.setRt(totalElapsedTime);
            }

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if(conn!=null){
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        return ret;
    }
}
