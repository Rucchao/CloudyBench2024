package com.cloudybench.workload;

import com.cloudybench.ConfigLoader;
import com.cloudybench.dbconn.ConnectionMgr;
import com.cloudybench.load.DateUtility;
import java.sql.*;
import java.util.Date;
import java.util.concurrent.ThreadLocalRandom;

public class CloudLagTime extends Client {
    int tp1_percent_lag = 80;
    int tp2_percent_lag = 15;
    int tp4_percent_lag = 5;

    //   RandomGenerator rg = new RandomGenerator();
    int customer_id = 0;
    int order_id = 0;
    int product_id = 0;
    int orderline_id = 0;
    double fresh_rate=0;
    // set init parameter before run
    @Override
    public void doInit() {
        tp1_percent_lag = intParameter("t1_percent_lag",80);
        tp2_percent_lag = intParameter("t2_percent_lag",15);
        tp4_percent_lag = intParameter("t4_percent_lag",5);

        if( (tp1_percent_lag + tp2_percent_lag + tp4_percent_lag) != 100 ){
            logger.error("TP analytical transaction percentage is not equal 100");
            System.exit(-1);
        }

        Long customernumer = CR.sales_customer_number;
        customer_id = customernumer.intValue() + 1;

        Long ordernumer = CR.sales_order_number;
        order_id = ordernumer.intValue() + 1;

        Long productnumer = CR.sales_product_number;
        product_id = productnumer.intValue() + 1;

        orderline_id = order_id*10;

        fresh_rate=Double.parseDouble(ConfigLoader.prop.getProperty("fresh_rate","0.5"));
    }

    // 3 Transactions

    // New Orderline
    public ClientResult execTxn1(Connection conn, Connection conn_replica) {

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
            int newid=0;
            int targetid=0;
            if (rs.next())
                newid = rs.getInt(1);
            //logger.info("The new id is "+newid);
            rs.close();
            pstmt.close();
            conn.commit();

            // get the data from the replica
            pstmt_replica = conn_replica.prepareStatement(statements[2]);

//            // tuning the orderline id with fresh_rate
//            double rand = rg.getRandomDouble();
//            if(rand<fresh_rate){
//                targetid=newid;
//            }
//            else
//                targetid = rg.getRandomint(1,orderline_id);
            pstmt_replica.setInt(1,newid);

            // count the lag time
            boolean Islag=false;
            int replicaid=0;
            ResultSet rs2 = pstmt_replica.executeQuery();// max id
            if (rs2.next())
                replicaid = rs2.getInt(1);// get the record
            //logger.info("The replica id is "+replicaid);

            // record the lag time
            long lagStartTs = System.currentTimeMillis();

            while (replicaid==0){
                logger.info("[Insert]: the replica data is stale!");
                Islag=true;
                rs2 = pstmt_replica.executeQuery();// get the fresh record
                if (rs2.next())
                    replicaid = rs2.getInt(1);
            }

            long lagEndTs = System.currentTimeMillis();

            if(Islag){
                long lag=lagEndTs-lagStartTs;
                logger.info("the lag time is "+lag+ " ms.");
                lagtime.add(lag);
            }

            rs2.close();
            pstmt_replica.close();
            conn_replica.commit();

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
    public ClientResult execTxn2(Connection conn, Connection conn_replica) {

        ClientResult cr = new ClientResult();
        PreparedStatement pstmt[] = new PreparedStatement[3];
        PreparedStatement pstmt_replica = null;
        ResultSet rs = null;
        long responseTime = 0L;
        java.sql.Timestamp ts = null;
        try {
            long currentStarttTs = System.currentTimeMillis();
            int O_ID = rg.getRandomint(1, order_id);
            int O_C_ID = 0;
            double O_TOTALAMOUNT = 0;
            String[] statements = sqls.tp_txn2();
            pstmt[0] = conn.prepareStatement(statements[0]);
            pstmt[1] = conn.prepareStatement(statements[1]);
            pstmt[2] = conn.prepareStatement(statements[2]);

            // transaction begins
            conn.setAutoCommit(false);
            // get order info

            pstmt[0].setInt(1,O_ID);
            rs = pstmt[0].executeQuery();
            if (rs.next()) {
                O_ID = rs.getInt(1);
                O_C_ID = rs.getInt(2);
                O_TOTALAMOUNT = rs.getDouble(3);
                ts = rs.getTimestamp(4);
            }
            rs.close();

            Date date = new Date(ts.getTime());
            Date new_date = DateUtility.OneDayAfter(date);
            Timestamp new_ts = new Timestamp(new_date.getTime());
           // logger.info("The primary ts is "+ new_ts);

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

            // get the data from the replica
            pstmt_replica = conn_replica.prepareStatement(statements[3]);
            pstmt_replica.setInt(1, O_ID);

            // count the lag time
            boolean Islag=false;
            Timestamp fresh_ts=null;
            ResultSet rs2 = pstmt_replica.executeQuery();
            if (rs2.next())
                 fresh_ts= rs2.getTimestamp(1);// get the record
            //logger.info("The replica ts is "+fresh_ts);

            // record the lag time
            long lagStartTs = System.currentTimeMillis();

            while (!fresh_ts.equals(new_ts)){
                logger.info("[Update]: the replica data is stale!");
                Islag=true;
                rs2 = pstmt_replica.executeQuery();// get the fresh record
                if (rs2.next())
                    fresh_ts = rs2.getTimestamp(1);
            }

            long lagEndTs = System.currentTimeMillis();

            if(Islag){
                long lag=lagEndTs-lagStartTs;
                logger.info("the lag time is "+lag+ " ms.");
                lagtime.add(lag);
            }

            rs2.close();
            pstmt_replica.close();
            conn_replica.commit();

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

    // Order Deletion
    public ClientResult execTxn4(Connection conn, Connection conn_replica) {

        ClientResult cr = new ClientResult();
        PreparedStatement pstmt[] = new PreparedStatement[2];
        PreparedStatement pstmt_replica = null;
        ResultSet rs = null;
        long responseTime = 0L;
        java.sql.Timestamp ts = null;
        try {
            long currentStarttTs = System.currentTimeMillis();
            int OL_ID = 0;
            String[] statements = sqls.tp_txn4();
            pstmt[0] = conn.prepareStatement(statements[0]);
            pstmt[1] = conn.prepareStatement(statements[1]);
            // transaction begins
            conn.setAutoCommit(false);
            // get order info
            rs = pstmt[0].executeQuery();
            if (rs.next()) {
                OL_ID = rs.getInt(1);
            }
            rs.close();

            // delete the order
            pstmt[1].setInt(1, OL_ID);
            pstmt[1].executeUpdate();
            //logger.info("The record " + OL_ID + " has been deleted!");
            conn.commit();

            // get the data from the replica
            pstmt_replica = conn_replica.prepareStatement(statements[2]);
            pstmt_replica.setInt(1, OL_ID);

            // count the lag time
            boolean Islag=false;
            ResultSet rs2 = pstmt_replica.executeQuery();

            // record the lag time
            long lagStartTs = System.currentTimeMillis();

            while (rs2.next()){
                logger.info("[Delete]: the replica data is stale!");
                Islag=true;
                rs2 = pstmt_replica.executeQuery();// get the fresh record
            }

            long lagEndTs = System.currentTimeMillis();

            if(Islag){
                long lag=lagEndTs-lagStartTs;
                logger.info("the lag time is "+lag+ " ms.");
                lagtime.add(lag);
            }

            conn_replica.commit();
            rs2.close();

            long currentEndTs = System.currentTimeMillis();
            responseTime = currentEndTs - currentStarttTs;
            hist.getTPItem(3).addValue(responseTime);
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
                pstmt_replica.close();
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
        Connection conn = ConnectionMgr.getConnection();
        Connection conn_replica = ConnectionMgr.getReplicaConnection();

        long totalElapsedTime = 0L;
        try {
            Class<CloudLagTime> tpClass = (Class<CloudLagTime>)Class.forName("com.cloudybench.workload.CloudLagTime");
            if(type == 1){
                while(!exitFlag) {
                    // cr = execTxn1(conn);
                    int rand = ThreadLocalRandom.current().nextInt(1, 100);
                    if(rand < tp1_percent_lag){
                        cr = execTxn1(conn, conn_replica);
                    }
                    else if(rand < tp1_percent_lag + tp2_percent_lag){
                        cr = execTxn2(conn,conn_replica);
                    }
                    else if(rand < tp1_percent_lag + tp2_percent_lag + tp4_percent_lag){
                        cr = execTxn4(conn,conn_replica);
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