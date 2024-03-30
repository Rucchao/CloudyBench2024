package com.cloudybench.workload;
/**
 *
 * @time 2024-01-12
 * @version 1.0.0
 * @file TPClient.java
 * @description
 *   TP Client Processor,include 6 TP transactions
 **/

import com.cloudybench.dbconn.ConnectionMgr;
import com.cloudybench.load.DateUtility;

import java.sql.*;
import java.util.Date;
import java.util.concurrent.ThreadLocalRandom;

public class CloudTPClient extends Client {
    int tp1_percent = 15;
    int tp2_percent = 5;
    int tp3_percent = 80;

    //   RandomGenerator rg = new RandomGenerator();
    int customer_id = 0;
    int order_id = 0;
    int product_id = 0;
    // set init parameter before run
    @Override
    public void doInit() {
        tp1_percent = intParameter("t1_percent_" + tenant_num,15);
        tp2_percent = intParameter("t2_percent_" + tenant_num,5);
        tp3_percent = intParameter("t3_percent_" + tenant_num,80);

        if( (tp1_percent + tp2_percent + tp3_percent) != 100 ){
            logger.error("TP analytical transaction percentage is not equal 100");
            System.exit(-1);
        }

        Long customernumer = CR.sales_customer_number;
        customer_id = customernumer.intValue() + 1;

        Long ordernumer = CR.sales_order_number;
        order_id = ordernumer.intValue() + 1;

        Long productnumer = CR.sales_product_number;
        product_id = productnumer.intValue() + 1;
    }

    // 3 Transactions

    // New Orderline
    public ClientResult execTxn1(Connection conn) {

        ClientResult cr = new ClientResult();
        // add a new orderline
        int o_id= rg.getRandomint(1, order_id);
        int p_id = rg.getRandomint(1,product_id);
        int OL_QUANTITY= rg.getRandomint(1,5);
        double OL_AMOUNT=rg.getRandomDouble(10.0);
        Date date = rg.getRandomTimestamp(CR.midPointDate, CR.endDate);
        java.sql.Timestamp ts = new Timestamp(date.getTime());
        PreparedStatement pstmt = null;
        long responseTime = 0L;
        try {
            long currentStarttTs = System.currentTimeMillis();
            // transaction begins
            conn.setAutoCommit(false);

            pstmt = conn.prepareStatement(sqls.tp_txn1());
            pstmt.setInt(1, o_id);
            pstmt.setInt(2, p_id);
            pstmt.setInt(3, OL_QUANTITY);
            pstmt.setDouble(4, OL_AMOUNT);
            pstmt.setTimestamp(5, ts);
            pstmt.executeUpdate();
            pstmt.close();
            conn.commit();

            long currentEndTs = System.currentTimeMillis();
            responseTime = currentEndTs - currentStarttTs;
            hist.getTPItem(0).addValue(responseTime);
            lock.lock();
            //logger.info("current tenant_num is "+tenant_num);
            tpTotalList[tenant_num-1]++;
            lock.unlock();
            cr.setRt(responseTime);
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

    // Payment
    public ClientResult execTxn2(Connection conn) {

        ClientResult cr = new ClientResult();
        PreparedStatement pstmt[] = new PreparedStatement[3];
        ;
        ResultSet rs = null;
        long responseTime = 0L;
        java.sql.Timestamp ts = null;
        try {
            long currentStarttTs = System.currentTimeMillis();
            int O_ID = 0;
            int O_C_ID = 0;
            double O_TOTALAMOUNT = 0;
            String[] statements = sqls.tp_txn2();
            pstmt[0] = conn.prepareStatement(statements[0]);
            pstmt[1] = conn.prepareStatement(statements[1]);
            pstmt[2] = conn.prepareStatement(statements[2]);
            // transaction begins
            conn.setAutoCommit(false);
            // get order info
            rs = pstmt[0].executeQuery();
            while (rs.next()) {
                O_ID = rs.getInt(1);
                O_C_ID = rs.getInt(2);
                O_TOTALAMOUNT = rs.getDouble(3);
                ts = rs.getTimestamp(4);
                break;
            }
            rs.close();

            Date date = new Date(ts.getTime());
            Date new_date = DateUtility.OneDayAfter(date);
            Timestamp new_ts = new Timestamp(new_date.getTime());

            // update customer's credit
            pstmt[1].setDouble(1, O_TOTALAMOUNT);
            pstmt[1].setTimestamp(2, new_ts);
            pstmt[1].setInt(3, O_C_ID);
            pstmt[1].executeUpdate();
            pstmt[1].close();

            // update order's updateddate
            pstmt[2].setTimestamp(1, new_ts);
            pstmt[2].setInt(2, O_ID);
            pstmt[2].executeUpdate();
            pstmt[2].close();
            conn.commit();

            long currentEndTs = System.currentTimeMillis();
            responseTime = currentEndTs - currentStarttTs;
            hist.getTPItem(1).addValue(responseTime);
            lock.lock();
            tpTotalList[tenant_num - 1]++;
            lock.unlock();
            cr.setRt(responseTime);
        }  catch (SQLException e) {
            e.printStackTrace();
            cr.setResult(false);
            cr.setErrorMsg(e.getMessage());
            cr.setErrorCode(String.valueOf(e.getErrorCode()));
        }  finally {
            try {
                pstmt[0].close();
                pstmt[1].close();
                pstmt[2].close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return cr;
    }

    // Order Status
    public ClientResult execTxn3(Connection conn) {
        ClientResult cr = new ClientResult();
        PreparedStatement pstmt = null;
        long responseTime = 0L;
        // transaction parameter
        int C_ID = rg.getRandomint(1,customer_id);
        try {
            long currentStarttTs = System.currentTimeMillis();
            // transaction begins
            conn.setAutoCommit(false);
            pstmt = conn.prepareStatement(sqls.tp_txn3());
            pstmt.setInt(1,C_ID);
            ResultSet rs = pstmt.executeQuery();
            rs.close();
            conn.commit();
            long currentEndTs = System.currentTimeMillis();
            responseTime = currentEndTs - currentStarttTs;
            hist.getTPItem(2).addValue(responseTime);
            lock.lock();
            tpTotalList[tenant_num-1]++;
            lock.unlock();
            cr.setRt(responseTime);
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


    public ClientResult execute() {
        int type = getTaskType();
        ClientResult ret = new ClientResult();
        ClientResult cr = null;

        // get the tenant url
        Connection conn = ConnectionMgr.getConnection(tenant_num,true);
        logger.info("This is tenant "+tenant_num);
        long totalElapsedTime = 0L;
        try {
            Class<CloudTPClient> tpClass = (Class<CloudTPClient>)Class.forName("com.cloudybench.workload.CloudTPClient");
            if(type == 8){
                while(!exitFlag) {
                    // cr = execTxn1(conn);
                    int rand = ThreadLocalRandom.current().nextInt(1, 100);
                    if(rand < tp1_percent){
                        cr = execTxn1(conn);
                    }
                    else if(rand < tp1_percent + tp2_percent){
                        cr = execTxn2(conn);
                    }
                    else if(rand < tp1_percent + tp2_percent + tp3_percent){
                        cr = execTxn3(conn);
                    }
                    totalElapsedTime += cr.getRt();
                    if(exitFlag)
                        break;
                }
                ret.setRt(totalElapsedTime);
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }finally {
            if(conn != null) {
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