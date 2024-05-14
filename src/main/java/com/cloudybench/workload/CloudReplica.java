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
import com.cloudybench.ConfigLoader;
import java.sql.*;
import java.util.Date;
import java.util.concurrent.ThreadLocalRandom;

public class CloudReplica extends Client {
    int tp1_percent = 15;
    int tp2_percent = 5;
    int tp3_percent = 80;

    //   RandomGenerator rg = new RandomGenerator();
    int customer_id = 0;
    int order_id = 0;
    int product_id = 0;
    int contention_num=0;
    String dist;
    // set init parameter before run
    @Override
    public void doInit() {
        tp1_percent = intParameter("t1_percent",15);
        tp2_percent = intParameter("t2_percent",5);
        tp3_percent = intParameter("t3_percent",80);

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

        contention_num = Integer.parseInt(ConfigLoader.prop.getProperty("contention_num","101"));
        dist= ConfigLoader.prop.getProperty("dist","uniform");
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
        PreparedStatement pstmt_replica = null;
        long responseTime = 0L;
        try {
            long currentStarttTs = System.currentTimeMillis();
            // transaction begins
            conn.setAutoCommit(false);
            String[] statements=sqls.tp_txn1();
            pstmt = conn.prepareStatement(statements[0]);
            pstmt.setInt(1, o_id);
            pstmt.setInt(2, p_id);
            pstmt.setInt(3, OL_QUANTITY);
            pstmt.setDouble(4, OL_AMOUNT);
            pstmt.setTimestamp(5, ts);
            pstmt.executeUpdate();

            pstmt=conn.prepareStatement(statements[1]);
            ResultSet rs = pstmt.executeQuery();
            rs.close();
            pstmt.close();
            conn.commit();

            long currentEndTs = System.currentTimeMillis();
            responseTime = currentEndTs - currentStarttTs;
            hist.getTPItem(0).addValue(responseTime);
            lock.lock();
            tpTotalCount++;
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

            // tuning the o_id distribution
            if (dist.equals("uniform"))
                O_ID= rg.getRandomint(1, order_id);
            else if (dist.equals("latest"))
                O_ID =rg.getRandomint(1,contention_num);

            // get order info
            pstmt[0].setInt(1,O_ID);
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

            // update order's updateddate
            pstmt[1].setTimestamp(1, new_ts);
            pstmt[1].setInt(2, O_ID);
            pstmt[1].executeUpdate();
            pstmt[1].close();

            // update customer's credit
            pstmt[2].setDouble(1, O_TOTALAMOUNT);
            pstmt[2].setTimestamp(2, new_ts);
            pstmt[2].setInt(3, O_C_ID);
            pstmt[2].executeUpdate();
            pstmt[2].close();
            conn.commit();

            long currentEndTs = System.currentTimeMillis();
            responseTime = currentEndTs - currentStarttTs;
            hist.getTPItem(1).addValue(responseTime);
            lock.lock();
            tpTotalCount++;
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

        int O_ID =0;

        // tuning the o_id distribution
        if (dist.equals("uniform"))
            O_ID= rg.getRandomint(1, order_id);
        else if (dist.equals("latest"))
            O_ID =rg.getRandomint(1,contention_num);

        try {
            long currentStarttTs = System.currentTimeMillis();
            // transaction begins
            conn.setAutoCommit(false);
            pstmt = conn.prepareStatement(sqls.tp_txn3());
            pstmt.setInt(1,O_ID);
            ResultSet rs = pstmt.executeQuery();
            rs.close();
            conn.commit();
            long currentEndTs = System.currentTimeMillis();
            responseTime = currentEndTs - currentStarttTs;
            hist.getTPItem(2).addValue(responseTime);
            lock.lock();
            tpTotalCount++;
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

        // get the primary and replica url
        Connection conn = ConnectionMgr.getConnection();
        Connection conn_replica = ConnectionMgr.getReplicaConnection();
        long totalElapsedTime = 0L;
        try {
            if(type == 2){
                while(!exitFlag) {
                    int rand = ThreadLocalRandom.current().nextInt(1, 100);
                    if(rand < tp1_percent){
                        cr = execTxn1(conn);
                    }
                    else if(rand < tp1_percent + tp2_percent){
                        cr = execTxn2(conn);
                    }
                    else if(rand < tp1_percent + tp2_percent + tp3_percent){
                        // get a random replica connection

                        cr = execTxn3(conn_replica);
                    }
                    totalElapsedTime += cr.getRt();
                    if(exitFlag)
                        break;
                }
                ret.setRt(totalElapsedTime);
            }
            if(type == 3){
                while(!exitFlag) {
                    int rand = ThreadLocalRandom.current().nextInt(1, 100);
                    if(rand < tp1_percent){
                        cr = execTxn1(conn);
                    }
                    else if(rand < tp1_percent + tp2_percent){
                        cr = execTxn2(conn);
                    }
                    else if(rand < tp1_percent + tp2_percent + tp3_percent){
                        // get a random replica connection

                        cr = execTxn3(conn);
                    }
                    totalElapsedTime += cr.getRt();
                    if(exitFlag)
                        break;
                }
                ret.setRt(totalElapsedTime);
            }

        } catch (Exception e) {
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