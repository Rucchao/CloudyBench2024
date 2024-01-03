package com.hybench.workload;
/**
 *
 * @time 2023-03-04
 * @version 1.0.0
 * @file TPClient.java
 * @description
 *   TP Client Processor,include TP transactions and AT transactions
 **/

import com.hybench.ConfigLoader;
import com.hybench.dbconn.ConnectionMgr;
import com.hybench.util.RandomGenerator;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.*;
import java.time.Instant;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;


public class CloudTPClient extends Client {
    int at1_percent = 35;
    int at2_percent = 25;
    int at3_percent = 15;
    int at4_percent = 15;
    int at5_percent = 7;
    int at6_percent = 3;

    int fresh_percent = 50;
    int tp1_percent = 5;
    int tp2_percent = 3;
    int tp3_percent = 5;
    int tp4_percent = 5;
    int tp5_percent = 3;
    int tp6_percent = 3;
    int tp7_percent = 3;
    int tp8_percent = 3;
    int tp9_percent = 7;
    int tp10_percent = 7;
    int tp11_percent = 7;
    int tp12_percent = 7;
    int tp13_percent = 6;
    int tp14_percent = 4;
    int tp15_percent = 4;
    int tp16_percent = 4;
    int tp17_percent = 12;
    int tp18_percent = 12;

    //   RandomGenerator rg = new RandomGenerator();
    int customer_no = 0;
    int company_no = 0;
    int contention_num=0;
    // set init parameter before run
    @Override
    public void doInit() {
        at1_percent = intParameter("at1_percent",35);
        at2_percent = intParameter("at2_percent",25);
        at3_percent = intParameter("at3_percent",15);
        at4_percent = intParameter("at4_percent",15);
        at5_percent = intParameter("at5_percent",7);
        at6_percent = intParameter("at6_percent",3);

        fresh_percent = intParameter("fresh_percent",50);

        tp1_percent = intParameter("tp1_percent",5);
        tp2_percent = intParameter("tp2_percent",3);
        tp3_percent = intParameter("tp3_percent",5);
        tp4_percent = intParameter("tp4_percent",5);
        tp5_percent = intParameter("tp5_percent",3);
        tp6_percent = intParameter("tp6_percent",3);
        tp7_percent = intParameter("tp7_percent",3);
        tp8_percent = intParameter("tp8_percent",3);
        tp9_percent = intParameter("tp9_percent",7);
        tp10_percent = intParameter("tp10_percent",7);
        tp11_percent = intParameter("tp11_percent",7);
        tp12_percent = intParameter("tp12_percent",7);
        tp13_percent = intParameter("tp13_percent",6);
        tp14_percent = intParameter("tp14_percent",4);
        tp15_percent = intParameter("tp15_percent",4);
        tp16_percent = intParameter("tp16_percent",4);
        tp17_percent = intParameter("tp17_percent",12);
        tp18_percent = intParameter("tp18_percent",12);

        if( (at1_percent + at2_percent + at3_percent + at4_percent + at5_percent + at6_percent) != 100 ){
            logger.error("TP analytical transaction percentage is not equal 100");
            System.exit(-1);
        }

        if( (tp1_percent + tp2_percent + tp3_percent +
                tp4_percent + tp5_percent + tp6_percent +
                tp7_percent + tp8_percent + tp9_percent +
                tp10_percent + tp11_percent + tp12_percent +
                tp13_percent + tp14_percent + tp15_percent +
                tp16_percent + tp17_percent + tp18_percent) != 100 ){
            logger.error("TP transaction percentage is not equal 100");
            System.exit(-1);
        }

        Long customernumer = CR.customer_number;
        Long companynumber = CR.company_number;
        customer_no = customernumer.intValue() + 1;
        company_no = companynumber.intValue();
        contention_num = Integer.parseInt(ConfigLoader.prop.getProperty("contention_num","100"));
    }

    public int Get_blocked_transfer_Id(){
        int Id=0;
        Id=Related_Blocked_Transfer_ids.get(rg.getRandomint(Related_Blocked_Transfer_ids.size()));
        return Id;
    }

    public int Get_blocked_checking_Id(){
        int Id=0;
        Id=Related_Blocked_Checking_ids.get(rg.getRandomint(Related_Blocked_Checking_ids.size()));
        return Id;
    }

    public ClientResult execFresh(Connection conn) {
        ClientResult cr = new ClientResult();
        String type = null;

        int sourceid = rg.getRandomint(1,customer_no+company_no);
        // Get customized targetid

        int targetId =testid1;

        double amount = 0;
        if(sourceid<customer_no){
            type = rg.getRandomCustTransferType();
            amount = rg.getRandomDouble(CR.customer_savingbalance * 0.0001);
        }
        else{
            type = rg.getRandomCompanyTransferType();
            amount = rg.getRandomDouble(CR.company_savingbalance * 0.0001);
        }

        PreparedStatement pstmt = null;
        long responseTime = 0L;
        try {
            String[] statements= sqls.tp_at1();
            String sql1=statements[0];
            String sql2=statements[1];
            String sql3=statements[2];
            String sql4=statements[3];
            String sql5=statements[4];
            long currentStarttTs = System.currentTimeMillis();
            // transaction begins
            conn.setAutoCommit(false);

            // update the source balance
            pstmt= conn.prepareStatement(sql3);
            pstmt.setDouble(1,amount);
            pstmt.setInt(2,sourceid);
            pstmt.executeUpdate();

            // update the target balance
            pstmt= conn.prepareStatement(sql4);
            pstmt.setDouble(1,amount);
            pstmt.setInt(2,targetId);
            pstmt.executeUpdate();

            // Insert into the Transfer
            Date date = rg.getRandomTimestamp(CR.midPointDate, CR.endDate);
            java.sql.Timestamp ts = new Timestamp(date.getTime());

            pstmt= conn.prepareStatement(sql5);
            pstmt.setInt(1,sourceid);
            pstmt.setInt(2,targetId);
            pstmt.setDouble(3,amount);
            pstmt.setString(4,type);
            pstmt.setTimestamp(5, ts);

////            // TODO: Set it with UTC time
//            java.sql.Timestamp fresh_ts = new Timestamp(Instant.now().minusMillis(TimeUnit.HOURS.toMillis(8)).toEpochMilli());
//            pstmt.setTimestamp(6, ts);

            pstmt.executeUpdate();
            pstmt.close();
            conn.commit();
            long currentEndTs = System.currentTimeMillis();
            responseTime = currentEndTs - currentStarttTs;
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

    public ClientResult execFresh2(Connection conn){
        ClientResult cr = new ClientResult();
        String type = null;

        int targetId =testid2;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        long responseTime = 0L;
        try {
            String[] statements= sqls.tp_at0();
            String sql0=statements[0];
            String sql1=statements[1];

            long currentStarttTs = System.currentTimeMillis();
            // transaction begins
            conn.setAutoCommit(false);

            // get top 10 employees
            pstmt= conn.prepareStatement(sql0);
            pstmt.setInt(1,targetId);

            rs = pstmt.executeQuery();
            int stopId = rg.getRandomint(1,10);
            int idx = 1;
            int custId = 0;
            while(rs.next()){
                custId = rs.getInt(1);
                idx++;
                if(idx == stopId){
                    break;
                }
            }
            // update ts
            pstmt= conn.prepareStatement(sql1);
            pstmt.setInt(1,custId);
            pstmt.executeUpdate();

            pstmt.close();
            conn.commit();
            long currentEndTs = System.currentTimeMillis();
            responseTime = currentEndTs - currentStarttTs;
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

    public ClientResult execFresh3(Connection conn){
        ClientResult cr = new ClientResult();
        String type = null;

        int targetId =testid3;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        long responseTime = 0L;
        try {
            String[] statements= sqls.tp_at00();
            String sql0=statements[0];
            String sql1=statements[1];

            long currentStarttTs = System.currentTimeMillis();
            // transaction begins
            conn.setAutoCommit(false);

            // get top 10 employees
            pstmt= conn.prepareStatement(sql0);
            pstmt.setInt(1,targetId);

            rs = pstmt.executeQuery();
            int stopId = rg.getRandomint(1,10);
            int idx = 1;
            int custId = 0;
            while(rs.next()){
                custId = rs.getInt(1);
                idx++;
                if(idx == stopId){
                    break;
                }
            }
            // update ts
            pstmt= conn.prepareStatement(sql1);
            pstmt.setInt(1,custId);
            pstmt.executeUpdate();

            delete_map2.put(custId, System.currentTimeMillis());

            pstmt.close();
            conn.commit();
            long currentEndTs = System.currentTimeMillis();
            responseTime = currentEndTs - currentStarttTs;
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


    // 6 Analytical Transactions (AT)
    public ClientResult execAT1(Connection conn) {

        ClientResult cr = new ClientResult();
        String type = null;

        int sourceid = rg.getRandomint(1,customer_no+company_no);
        int targetId = 0;
        // Get a random blocked ids for targetid with the specified risk_rate
        double rand = rg.getRandomDouble();
        if(rand<risk_rate){
            targetId = Get_blocked_transfer_Id();
        }
        else
            targetId = rg.getRandomint(1,customer_no+company_no);

        double amount = 0;
        if(sourceid<customer_no){
            type = rg.getRandomCustTransferType();
            amount = rg.getRandomDouble(CR.customer_savingbalance * 0.0001);
        }
        else{
            type = rg.getRandomCompanyTransferType();
            amount = rg.getRandomDouble(CR.company_savingbalance * 0.0001);
        }

        PreparedStatement pstmt = null;
        ResultSet rs = null;
        long responseTime = 0L;
        try {
            String[] statements= sqls.tp_at1();
            String sql1=statements[0];
            String sql2=statements[1];
            String sql3=statements[2];
            String sql4=statements[3];
            String sql5=statements[4];

            long currentStarttTs = System.currentTimeMillis();
            // transaction begins
            conn.setAutoCommit(false);

            // Check if the targetid is a blocked user
            pstmt = conn.prepareStatement(sql1);
            pstmt.setInt(1,targetId);
            rs= pstmt.executeQuery();
            int flag = 0;
            double balance = 0;
            if (rs.next()){
                flag = rs.getInt(1);
                balance = rs.getDouble(2);
            }
            if (flag == 1 || balance < amount){
                conn.rollback();
            }
            else{
                pstmt = conn.prepareStatement(sql2);
                pstmt.setInt(1,targetId);
                rs= pstmt.executeQuery();
                int count = 0;
                if (rs.next()){
                    count = rs.getInt(1);
                }
                if (count > 0){
                    conn.rollback();
                }
                else {
                    // update the source balance
                    pstmt = conn.prepareStatement(sql3);
                    pstmt.setDouble(1, amount);
                    pstmt.setInt(2, sourceid);
                    pstmt.executeUpdate();

                    // update the target balance
                    pstmt = conn.prepareStatement(sql4);
                    pstmt.setDouble(1, amount);
                    pstmt.setInt(2, targetId);
                    pstmt.executeUpdate();

                    // Insert into the Transfer
                    Date date = rg.getRandomTimestamp(CR.midPointDate, CR.endDate);
                    java.sql.Timestamp ts = new Timestamp(date.getTime());

                    pstmt = conn.prepareStatement(sql5);
                    pstmt.setInt(1, sourceid);
                    pstmt.setInt(2, targetId);
                    pstmt.setDouble(3, amount);
                    pstmt.setString(4, type);
                    pstmt.setTimestamp(5, ts);
                    pstmt.executeUpdate();
                    pstmt.close();
                    rs.close();
                    conn.commit();
                }
            }

            // analyze the related blocked users

            long currentEndTs = System.currentTimeMillis();
            responseTime = currentEndTs - currentStarttTs;
            hist.getXPATItem(0).addValue(responseTime);
            lock.lock();
            atTotalCount++;
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

    public ClientResult execAT2(Connection conn ){
        ClientResult cr = new ClientResult();

        String type = null;

        // Get random sourceid
        int sourceid = 0;
        int targetId = rg.getRandomint(1,customer_no+company_no);

        double rand = rg.getRandomDouble();
        if(rand<risk_rate){
            sourceid = Get_blocked_checking_Id();
        }

        else
            sourceid = rg.getRandomint(1,customer_no+company_no);

        double amount = 0;
        if(sourceid<customer_no){
            type = rg.getRandomCustTransferType();
            amount = rg.getRandomDouble(CR.customer_checkingbalance * 0.0001);
        }
        else{
            type = rg.getRandomCompanyTransferType();
            amount = rg.getRandomDouble(CR.company_checkingbalance * 0.0001);
        }

        PreparedStatement pstmt = null;
        ResultSet rs = null;
        long responseTime = 0L;
        try {
            String[] statements= sqls.tp_at2();

            String sql1=statements[0];
            String sql2=statements[1];
            String sql3=statements[2];
            String sql4=statements[3];
            String sql5=statements[4];

            long currentStarttTs = System.currentTimeMillis();
            // transaction begins
            conn.setAutoCommit(false);
            // Check if the sourceid is a blocked user
            pstmt = conn.prepareStatement(sql1);
            pstmt.setInt(1,sourceid);
            rs= pstmt.executeQuery();
            int flag = 0;
            double balance = 0;
            if (rs.next()){
                flag = rs.getInt(1);
                balance = rs.getInt(2);
            }
            if (flag==1 || balance<amount){
                conn.rollback();
            }
            else{
                pstmt = conn.prepareStatement(sql2);
                pstmt.setInt(1,targetId);
                rs= pstmt.executeQuery();
                int count = 0 ;
                if (rs.next()){
                    count = rs.getInt(1);
                }
                if (count>0){
                    conn.rollback();
                }
                else{
                    // update the source balance
                    pstmt= conn.prepareStatement(sql3);
                    pstmt.setDouble(1,amount);
                    pstmt.setInt(2,sourceid);
                    pstmt.executeUpdate();

                    // update the target balance
                    pstmt= conn.prepareStatement(sql4);
                    pstmt.setDouble(1,amount);
                    pstmt.setInt(2,targetId);
                    pstmt.executeUpdate();

                    // Insert into the Check
                    Date date = rg.getRandomTimestamp(CR.midPointDate, CR.endDate);
                    java.sql.Timestamp ts = new Timestamp(date.getTime());

                    pstmt= conn.prepareStatement(sql5);
                    pstmt.setInt(1,sourceid);
                    pstmt.setInt(2,targetId);
                    pstmt.setDouble(3,amount);
                    pstmt.setString(4,type);
                    pstmt.setTimestamp(5, ts);
                    pstmt.executeUpdate();
                    pstmt.close();
                    rs.close();
                    conn.commit();
                }
            }

            long currentEndTs = System.currentTimeMillis();
            responseTime = currentEndTs - currentStarttTs;
            hist.getXPATItem(1).addValue(responseTime);
            lock.lock();
            atTotalCount++;
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

    public ClientResult execAT3(Connection conn ){
        ClientResult cr = new ClientResult();

        ResultSet rs = null;
        Statement stmt = null;
        int AT3_applicantid = 0;
        int AT3_loanid = 0;
        int duration = 0;
        Date date = null;
        Timestamp old_ts = null;
        Timestamp new_ts = null;
        try{
            stmt = conn.createStatement();
            double rand = rg.getRandomDouble();
            // Get random sourceid
            rs = stmt.executeQuery( sqls.tp_at3_1());
            while (rs.next()) {
                AT3_applicantid= rs.getInt(1);
                AT3_loanid=rs.getInt(2);
                old_ts = rs.getTimestamp(3);
                if(old_ts.getTime()<CR.endDate.getTime())
                    date = rg.getRandomTimestamp(old_ts.getTime(), CR.endDate.getTime());
                else
                    date = CR.endDate;
                new_ts = new Timestamp(date.getTime());
                duration = rs.getInt(4);
                break;
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
            String[] statements= sqls.tp_at3();
            String sql1=statements[0];
            String sql2=statements[1];
            String sql3=statements[2];
            String sql4=statements[3];
            String sql5=statements[4];
            long currentStarttTs = System.currentTimeMillis();
            pstmt = conn.prepareStatement(sql1);
            pstmt.setInt(1,AT3_applicantid);
            rs = pstmt.executeQuery();
            int flag = 0;
            if (rs.next()){
                flag = rs.getInt(1);
            }
            if (flag==1){
                pstmt = conn.prepareStatement(sql5);
                pstmt.setTimestamp(1,new_ts);
                pstmt.setInt(2,AT3_loanid);
                pstmt.executeUpdate();
                conn.commit();
            }
            else{
                pstmt = conn.prepareStatement(sql2);
                pstmt.setInt(1,AT3_applicantid);
                pstmt.setInt(2,AT3_applicantid);
                rs = pstmt.executeQuery();
                int amount = 0;
                while (rs.next()){
                    amount = rs.getInt(1);
                }
                if (amount<0){
                    // reject the loanapps
                    pstmt = conn.prepareStatement(sql5);
                    pstmt.setTimestamp(1,new_ts);
                    pstmt.setInt(2,AT3_loanid);
                    pstmt.executeUpdate();
                }
                else{
                    // insert the loantrans
                    pstmt= conn.prepareStatement(sql3);
                    // Insert into the LOANTRANS
                    pstmt.setInt(1, AT3_applicantid);
                    pstmt.setInt(2, AT3_loanid);
                    pstmt.setDouble(3, amount);
                    pstmt.setString(4, "accept");
                    pstmt.setTimestamp(5, old_ts);
                    pstmt.setInt(6, duration);
                    pstmt.setTimestamp(7, new_ts);
                    pstmt.setInt(8, 0);
                    pstmt.execute();

                    // accept the loanapps status
                    pstmt= conn.prepareStatement(sql4);
                    pstmt.setTimestamp(1,new_ts);
                    pstmt.setInt(2,AT3_loanid);
                    pstmt.executeUpdate();
                }

                conn.commit();
            }

            long currentEndTs = System.currentTimeMillis();
            responseTime = currentEndTs - currentStarttTs;
            hist.getXPATItem(2).addValue(responseTime);
            lock.lock();
            atTotalCount++;
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

    public ClientResult execAT4(Connection conn ){

        ClientResult cr = new ClientResult();

        Statement stmt = null;
        int AT4_applicantid=0;
        double AT4_amount=0;
        int AT4_loanid=0;
        Timestamp old_ts = null;
        Timestamp new_ts = null;
        Date date = null;
        try{
            stmt = conn.createStatement();
            // Get random sourceid
            ResultSet rs = stmt.executeQuery(sqls.tp_at4_1());
            while (rs.next()) {
                AT4_applicantid= rs.getInt(1);
                AT4_amount = rs.getDouble(2);
                AT4_loanid = rs.getInt(3);
                old_ts = rs.getTimestamp(4);
                if(old_ts != null && old_ts.getTime()<CR.endDate.getTime() )
                    date = rg.getRandomTimestamp(old_ts.getTime(), CR.endDate.getTime());
                else
                    date = CR.endDate;
                new_ts = new Timestamp(date.getTime());
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
        ResultSet rs = null;
        long responseTime = 0L;
        try {
            String[] statements= sqls.tp_at4();

            String sql1=statements[0];
            String sql2=statements[1];
            String sql3=statements[2];
            long currentStarttTs = System.currentTimeMillis();

            // transaction begins
            conn.setAutoCommit(false);
            // Check if the sourceid is a blocked user
            pstmt = conn.prepareStatement(sql1);
            pstmt.setInt(1,AT4_applicantid);
            rs= pstmt.executeQuery();
            while (rs.next()){
                int flag = rs.getInt(1);
                // current threshold is set to 5
                if (flag>5){
                    conn.rollback();
                    long currentEndTs = System.currentTimeMillis();
                    responseTime = currentEndTs - currentStarttTs;
                    hist.getXPATItem(3).addValue(responseTime);
                    lock.lock();
                    tpTotalCount++;
                    lock.unlock();
                    cr.setRt(responseTime);
                    return cr;
                }
            }

            // lend the loan amount
            pstmt= conn.prepareStatement(sql2);
            pstmt.setDouble(1,AT4_amount);
            pstmt.setInt(2,AT4_applicantid);
            pstmt.executeUpdate();

            // update the loan status
            pstmt= conn.prepareStatement(sql3);
            pstmt.setTimestamp(1,new_ts);
            pstmt.setInt(2,AT4_loanid);
            pstmt.executeUpdate();

            pstmt.close();
            rs.close();
            conn.commit();
            long currentEndTs = System.currentTimeMillis();
            responseTime = currentEndTs - currentStarttTs;
            hist.getXPATItem(3).addValue(responseTime);
            lock.lock();
            atTotalCount++;
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

    public ClientResult execAT5(Connection conn ){
        ClientResult cr = new ClientResult();

        Statement stmt = null;
        int AT5_applicantid = 0 ;
        try{
            stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery( sqls.tp_at5_1());
            if (rs.next()) {
                AT5_applicantid= rs.getInt(1);
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

        PreparedStatement pstmt1 = null;
        ResultSet rs = null;
        long responseTime = 0L;
        try {
            String[] statements= sqls.tp_at5();
            String sql1=statements[0];
            String sql2=statements[1];

            long currentStarttTs = System.currentTimeMillis();
            // transaction begins
            conn.setAutoCommit(false);
            // Check if the user has any delinquency records
            pstmt1 = conn.prepareStatement(sql1);
            pstmt1.setInt(1,AT5_applicantid);
            rs= pstmt1.executeQuery();
            while(rs.next()){
                PreparedStatement pstmt2=conn.prepareStatement(sql2);
                int id = rs.getInt(1);
                pstmt2.setInt(1,id);
                pstmt2.executeUpdate();
            }
            conn.commit();
            long currentEndTs = System.currentTimeMillis();
            responseTime = currentEndTs - currentStarttTs;
            hist.getXPATItem(4).addValue(responseTime);
            lock.lock();
            atTotalCount++;
            lock.unlock();
            cr.setRt(responseTime);
        } catch (SQLException e) {
            e.printStackTrace();
            cr.setResult(false);
            cr.setErrorMsg(e.getMessage());
            cr.setErrorCode(String.valueOf(e.getErrorCode()));
        }  finally {
            try {
                pstmt1.close();

            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return cr;
    }

    public ClientResult execAT6(Connection conn ){
        ClientResult cr = new ClientResult();

        Statement stmt = null;
        int AT6_applicantid = 0 ;
        try{
            stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(sqls.tp_at6_1());
            if (rs.next()) {
                AT6_applicantid= rs.getInt(1);
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


        PreparedStatement pstmt1 = null;
        ResultSet rs = null;
        long responseTime = 0L;
        try {
            String[] statements= sqls.tp_at6();
            String sql1=statements[0];
            String sql2=statements[1];
            String sql3=statements[2];

            long currentStarttTs = System.currentTimeMillis();
            // transaction begins
            conn.setAutoCommit(false);
            // Check if there exists users have any delinquency records over 1 month
            pstmt1 = conn.prepareStatement(sql1);
            pstmt1.setInt(1, AT6_applicantid);
            rs= pstmt1.executeQuery();
            while(rs.next()){
                int count = rs.getInt(1);
                if (count>0) {
                    // block the savingaccount
                    PreparedStatement pstmt2 = conn.prepareStatement(sql2);
                    pstmt2.setInt(1, AT6_applicantid);
                    pstmt2.executeUpdate();

                    // block the checkingaccount
                    PreparedStatement pstmt3 = conn.prepareStatement(sql3);
                    pstmt3.setInt(1, AT6_applicantid);
                    pstmt3.executeUpdate();

                    // add blocked accountid for IQ2
                    while(queue_ids.offer(AT6_applicantid)==false){
                        queue_ids.poll();
                    }
                }
                else{
                    conn.rollback();
                }
            }
            conn.commit();
            long currentEndTs = System.currentTimeMillis();
            responseTime = currentEndTs - currentStarttTs;
            hist.getXPATItem(5).addValue(responseTime);
            lock.lock();
            atTotalCount++;
            lock.unlock();
            cr.setRt(responseTime);
        } catch (SQLException e) {
            e.printStackTrace();
            cr.setResult(false);
            cr.setErrorMsg(e.getMessage());
            cr.setErrorCode(String.valueOf(e.getErrorCode()));
        }  finally {
            try {
                rs.close();
                pstmt1.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return cr;
    }

    // 18 Operational Transactions (OT)
    public ClientResult execTxn1(Connection conn) {

        ClientResult cr = new ClientResult();

        PreparedStatement pstmt = null;
        long responseTime = 0L;
        // transaction parameter;
        int custid =  rg.getRandomint(1,customer_no);
        try {
            long currentStarttTs = System.currentTimeMillis();
            // transaction begins
            conn.setAutoCommit(false);
            pstmt = conn.prepareStatement(sqls.tp_txn1());
            pstmt.setInt(1,custid);
            ResultSet rs = pstmt.executeQuery();
            rs.close();
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

    public ClientResult execTxn2(Connection conn ) {

        ClientResult cr = new ClientResult();

        PreparedStatement pstmt = null;
        long responseTime = 0L;
        // transaction parameter;
        int companyid = rg.getRandomint(customer_no,customer_no+company_no);

        try {
            long currentStarttTs = System.currentTimeMillis();
            // transaction begins
            conn.setAutoCommit(false);
            pstmt = conn.prepareStatement(sqls.tp_txn2());
            pstmt.setInt(1, companyid);
            ResultSet rs = pstmt.executeQuery();
            rs.close();

            conn.commit();
            long currentEndTs = System.currentTimeMillis();
            responseTime = currentEndTs - currentStarttTs;
            hist.getTPItem(1).addValue(responseTime);
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

    public ClientResult execTxn3(Connection conn ) {

        ClientResult cr = new ClientResult();
        PreparedStatement pstmt = null;
        long responseTime = 0L;
        // transaction parameter
        int accountId = rg.getRandomint(1,customer_no+company_no);
        try {
            long currentStarttTs = System.currentTimeMillis();
            // transaction begins
            conn.setAutoCommit(false);
            pstmt = conn.prepareStatement(sqls.tp_txn3());
            pstmt.setInt(1,accountId);
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

    public ClientResult execTxn4(Connection conn ) {
        ClientResult cr = new ClientResult();

        PreparedStatement pstmt = null;
        long responseTime = 0L;
        // transaction parameter
        int accountId = rg.getRandomint(1,customer_no+company_no);
        try {
            long currentStarttTs = System.currentTimeMillis();
            // transaction begins
            conn.setAutoCommit(false);
            pstmt = conn.prepareStatement(sqls.tp_txn4());
            pstmt.setInt(1,accountId);
            ResultSet rs = pstmt.executeQuery();
            rs.close();
            conn.commit();
            long currentEndTs = System.currentTimeMillis();
            responseTime = currentEndTs - currentStarttTs;
            hist.getTPItem(3).addValue(responseTime);
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

    public ClientResult execTxn5(Connection conn) {
        ClientResult cr = new ClientResult();
        PreparedStatement pstmt = null;
        long responseTime = 0L;
        // transaction parameter
        int accountId = rg.getRandomint(1,customer_no+company_no);
        try {
            long currentStarttTs = System.currentTimeMillis();
            // transaction begins
            conn.setAutoCommit(false);
            pstmt = conn.prepareStatement(sqls.tp_txn5());
            pstmt.setInt(1,accountId);
            pstmt.setInt(2,accountId);
            ResultSet rs = pstmt.executeQuery();
            rs.close();
            conn.commit();
            long currentEndTs = System.currentTimeMillis();
            responseTime = currentEndTs - currentStarttTs;
            hist.getTPItem(4).addValue(responseTime);
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

    public ClientResult execTxn6(Connection conn) {
        ClientResult cr = new ClientResult();
        PreparedStatement pstmt = null;
        long responseTime = 0L;
        // transaction parameter
        int accountId = rg.getRandomint(1,customer_no+company_no);
        try {
            long currentStarttTs = System.currentTimeMillis();
            // transaction begins
            conn.setAutoCommit(false);
            pstmt = conn.prepareStatement(sqls.tp_txn6());
            pstmt.setInt(1,accountId);
            pstmt.setInt(2,accountId);
            ResultSet rs = pstmt.executeQuery();
            rs.close();
            conn.commit();
            long currentEndTs = System.currentTimeMillis();
            responseTime = currentEndTs - currentStarttTs;
            hist.getTPItem(5).addValue(responseTime);
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

    public ClientResult execTxn7(Connection conn) {
        ClientResult cr = new ClientResult();
        PreparedStatement pstmt = null;
        long responseTime = 0L;
        // transaction parameter
        int accountId = rg.getRandomint(1,customer_no+company_no);
        try {
            long currentStarttTs = System.currentTimeMillis();
            // transaction begins
            conn.setAutoCommit(false);
            pstmt = conn.prepareStatement(sqls.tp_txn7());
            pstmt.setInt(1,accountId);
            ResultSet rs = pstmt.executeQuery();
            rs.close();

            conn.commit();
            long currentEndTs = System.currentTimeMillis();
            responseTime = currentEndTs - currentStarttTs;
            hist.getTPItem(6).addValue(responseTime);
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

    public ClientResult execTxn8(Connection conn) {
        ClientResult cr = new ClientResult();
        PreparedStatement pstmt = null;
        long responseTime = 0L;
        // transaction parameter
        int accountId = rg.getRandomint(1,customer_no+company_no);
        try {
            long currentStarttTs = System.currentTimeMillis();
            // transaction begins
            conn.setAutoCommit(false);
            pstmt = conn.prepareStatement(sqls.tp_txn8());
            pstmt.setInt(1,accountId);
            ResultSet rs = pstmt.executeQuery();
            rs.close();
            conn.commit();
            long currentEndTs = System.currentTimeMillis();
            responseTime = currentEndTs - currentStarttTs;
            hist.getTPItem(7).addValue(responseTime);
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

    public ClientResult execTxn9(Connection conn) {
        ClientResult cr = new ClientResult();
        // Get random sourceid
        int sourceid = rg.getRandomint(1,customer_no+company_no);
        // Get random targetid
        int targetId = rg.getRandomint(1,customer_no+company_no);
        String type =null;
        double amount = 0;
        if(sourceid<customer_no){
            type = rg.getRandomCustTransferType();
            amount = rg.getRandomDouble(CR.customer_savingbalance * 0.0001);
        }
        else{
            type = rg.getRandomCompanyTransferType();
            amount = rg.getRandomDouble(CR.company_savingbalance * 0.0001);
        }

        PreparedStatement pstmt[] = new PreparedStatement[4];
        ResultSet rs = null;

        long responseTime = 0L;

        try {
            String[] statements= sqls.tp_txn9();

            String sql1=statements[0];
            String sql2=statements[1];
            String sql3=statements[2];
            String sql4=statements[3];
            long currentStarttTs = System.currentTimeMillis();
            // transaction begins
            conn.setAutoCommit(false);
            double balance =0;
            pstmt[0] = conn.prepareStatement(sql1);
            pstmt[1]= conn.prepareStatement(sql2);
            pstmt[2]= conn.prepareStatement(sql3);
            pstmt[3]= conn.prepareStatement(sql4);

            pstmt[0].setInt(1,sourceid);
            rs= pstmt[0].executeQuery();
            if (rs.next()){
                balance = rs.getDouble(1);
            }
            rs.close();

            if (balance<amount){ // Check the balance
                conn.rollback();
            }else {
                // update the source balance
                pstmt[1].setDouble(1, amount);
                pstmt[1].setInt(2,sourceid);
                pstmt[1].executeUpdate();

                // update the target balance
                pstmt[2].setDouble(1, amount);
                pstmt[2].setInt(2,targetId);
                pstmt[2].executeUpdate();

                // Insert into the Transfer
                Date date = rg.getRandomTimestamp(CR.midPointDate, CR.endDate);
                java.sql.Timestamp ts = new Timestamp(date.getTime());
                pstmt[3].setInt(1,sourceid);
                pstmt[3].setInt(2,targetId);
                pstmt[3].setDouble(3,amount);
                pstmt[3].setString(4,type);
                pstmt[3].setTimestamp(5, ts);
                if(dbType == 5){
                    pstmt[3].setTimestamp(6,null);
                }
                pstmt[3].executeUpdate();

                conn.commit();
            }
            long currentEndTs = System.currentTimeMillis();
            responseTime = currentEndTs - currentStarttTs;
            hist.getTPItem(8).addValue(responseTime);
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
                pstmt[0].close();
                pstmt[1].close();
                pstmt[2].close();
                pstmt[3].close();

            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return cr;
    }

    public ClientResult execTxn10(Connection conn) {
        ClientResult cr = new ClientResult();
        // transaction parameter
        int companyid = rg.getRandomint(customer_no,customer_no+company_no);
        String type = "salary";
        // same salary
        double salary = rg.getRandomDouble(CR.company_savingbalance * 0.01);
        PreparedStatement pstmt[] = new PreparedStatement[4];
        ResultSet rs = null;
        long responseTime = 0L;
        try {
            String[] statements= sqls.tp_txn10();

            String sql1=statements[0];
            String sql2=statements[1];
            String sql3=statements[2];
            String sql4=statements[3];
            long currentStarttTs = System.currentTimeMillis();

            // transaction begins
            conn.setAutoCommit(false);
            pstmt[0] = conn.prepareStatement(sql1);
            pstmt[1] = conn.prepareStatement(sql2);
            pstmt[2] = conn.prepareStatement(sql3);
            pstmt[3] = conn.prepareStatement(sql4);

            //get all employees
            pstmt[0].setInt(1,companyid);
            rs= pstmt[0].executeQuery();
            while(rs.next()) {
                int custid = rs.getInt(1);

                // update company savingaccount
                pstmt[1].setDouble(1, salary);
                pstmt[1].setInt(2, companyid);
                pstmt[1].executeUpdate();
                // update employee savingaccount
                pstmt[2].setDouble(1, salary);
                pstmt[2].setInt(2, custid);
                pstmt[2].executeUpdate();
                Date date = rg.getRandomTimestamp(CR.midPointDate, CR.endDate);
                java.sql.Timestamp ts = new Timestamp(date.getTime());

                pstmt[3].setInt(1,companyid);
                pstmt[3].setInt(2,custid);
                pstmt[3].setDouble(3,salary);
                pstmt[3].setString(4,type);
                pstmt[3].setTimestamp(5, ts);
                pstmt[3].executeUpdate();
            }
            rs.close();
            conn.commit();
            long currentEndTs = System.currentTimeMillis();
            responseTime = currentEndTs - currentStarttTs;
            hist.getTPItem(9).addValue(responseTime);
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
                pstmt[0].close();
                pstmt[1].close();
                pstmt[2].close();
                pstmt[3].close();

            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return cr;
    }

    public ClientResult execTxn11(Connection conn) {
        ClientResult cr = new ClientResult();
        // Get random sourceid
        int sourceid = rg.getRandomint(1,customer_no+company_no);
        // Get random targetid
        int targetId = rg.getRandomint(1,customer_no+company_no);
        String type =null;
        double amount = 0;
        if(sourceid<customer_no){
            type = rg.getRandomCustTransferType();
            amount = rg.getRandomDouble(CR.customer_checkingbalance * 0.0001);
        }
        else{
            type = rg.getRandomCompanyTransferType();
            amount = rg.getRandomDouble(CR.company_checkingbalance * 0.0001);
        }

        PreparedStatement pstmt[] = new PreparedStatement[4];
        ResultSet rs = null;

        long responseTime = 0L;

        try {
            String[] statements= sqls.tp_txn11();
            String sql1=statements[0];
            String sql2=statements[1];
            String sql3=statements[2];
            String sql4=statements[3];
            long currentStarttTs = System.currentTimeMillis();

            // transaction begins
            conn.setAutoCommit(false);
            double balance =0;
            pstmt[0] = conn.prepareStatement(sql1);
            pstmt[1]= conn.prepareStatement(sql2);
            pstmt[2]= conn.prepareStatement(sql3);
            pstmt[3]= conn.prepareStatement(sql4);

            pstmt[0].setInt(1,sourceid);
            rs= pstmt[0].executeQuery();
            while (rs.next()){
                balance = rs.getDouble(1);
                break;
            }
            rs.close();

            if (balance<amount){ // Check the balance
                conn.rollback();
            }else {
                // update the source balance
                pstmt[1].setDouble(1, amount);
                pstmt[1].setInt(2,sourceid);
                pstmt[1].executeUpdate();

                // update the target balance
                pstmt[2].setDouble(1, amount);
                pstmt[2].setInt(2,targetId);
                pstmt[2].executeUpdate();

                // Insert into the CHECKING
                Date date = rg.getRandomTimestamp(CR.midPointDate, CR.endDate);
                java.sql.Timestamp ts = new Timestamp(date.getTime());

                pstmt[3].setInt(1,sourceid);
                pstmt[3].setInt(2,targetId);
                pstmt[3].setDouble(3,amount);
                pstmt[3].setString(4,type);
                pstmt[3].setTimestamp(5, ts);
                pstmt[3].executeUpdate();

                conn.commit();
            }
            long currentEndTs = System.currentTimeMillis();
            responseTime = currentEndTs - currentStarttTs;
            hist.getTPItem(10).addValue(responseTime);
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
                pstmt[0].close();
                pstmt[1].close();
                pstmt[2].close();
                pstmt[3].close();

            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return cr;
    }

    public ClientResult execTxn12(Connection conn) {
        ClientResult cr = new ClientResult();
        PreparedStatement pstmt[] = new PreparedStatement[3];
        ResultSet rs = null;
        long responseTime = 0L;

        // Get random applicantid
        int applicantid = rg.getRandomint(1,customer_no+company_no);

        double amount = 0;
        double balance = 0;
        if(applicantid<customer_no){
            amount = rg.getRandomDouble(CR.customer_loanbalance);
        }
        else{
            amount = rg.getRandomDouble(CR.company_loanbalance);
        }

        String status = "under_review";
        int duration = rg.getRandomLoanDuration();

        Date date = rg.getRandomTimestamp(CR.loanDate, CR.endDate);
        java.sql.Timestamp ts = new Timestamp(date.getTime());

        try {
            String[] statements= sqls.tp_txn12();
            String sql1=statements[0];
            String sql2=statements[1];
            String sql3=statements[2];
            String sql4=statements[3];
            String sql5=statements[4];
            long currentStarttTs = System.currentTimeMillis();
            // transaction begins
            conn.setAutoCommit(false);

            // it is a customer
            if(applicantid<customer_no) {
                pstmt[0]=conn.prepareStatement(sql1);
                pstmt[0].setInt(1,applicantid);
                rs= pstmt[0].executeQuery();
                while (rs.next()){
                    balance = rs.getDouble(1);
                    break;
                }
                rs.close();
                if (balance<amount){ // Check the balance
                    conn.rollback();
                }
                else{
                    // update the loan balance
                    pstmt[1]=conn.prepareStatement(sql2);
                    pstmt[1].setDouble(1, amount);
                    pstmt[1].setInt(2,applicantid);
                    pstmt[1].executeUpdate();
                }
            }
            // it is a company
            else{
                pstmt[0]=conn.prepareStatement(sql3);
                pstmt[0].setInt(1,applicantid);
                rs= pstmt[0].executeQuery();
                while (rs.next()){
                    balance = rs.getDouble(1);
                    break;
                }
                rs.close();
                if (balance<amount){ // Check the balance
                    conn.rollback();
                }
                else{
                    // update the loan balance
                    pstmt[1]=conn.prepareStatement(sql4);
                    pstmt[1].setDouble(1, amount);
                    pstmt[1].setInt(2,applicantid);
                    pstmt[1].executeUpdate();
                }
            }
            // insert the loan app
            pstmt[2]=conn.prepareStatement(sql5);
            pstmt[2].setInt(1, applicantid);
            pstmt[2].setDouble(2, amount);
            pstmt[2].setInt(3, duration);
            pstmt[2].setString(4,status);
            pstmt[2].setTimestamp(5, ts);
            pstmt[2].execute();
            conn.commit();
            long currentEndTs = System.currentTimeMillis();
            responseTime = currentEndTs - currentStarttTs;
            hist.getTPItem(11).addValue(responseTime);
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
                if(pstmt[0] != null)
                    pstmt[0].close();
                if(pstmt[1] != null)
                    pstmt[1].close();
                if(pstmt[2] != null)
                    pstmt[2].close();

            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return cr;
    }

    public ClientResult execTxn13(Connection conn) {
        ClientResult cr = new ClientResult();

        PreparedStatement pstmt[] = new PreparedStatement[5];
        ResultSet rs = null;
        long responseTime = 0L;
        try {
            String[] statements= sqls.tp_txn13();
            String sql1=statements[0];
            String sql2=statements[1];
            String sql3=statements[2];
            String sql4=statements[3];
            String sql5=statements[4];
            int appid=0;
            int applicantID = 0;
            int amount = 0;
            int duration =0;
            String status= null;

            // half for accept, half for status
            Random rand = new Random();
            double prob = rand.nextDouble();
            if(prob<0.5)
                status="accept";
            else
                status="reject";

            java.sql.Timestamp appts = null;
            java.sql.Timestamp contract_ts = null;
            long currentStarttTs = System.currentTimeMillis();

            // transaction begins
            conn.setAutoCommit(false);
            pstmt[0] = conn.prepareStatement(sql1);
            pstmt[1] = conn.prepareStatement(sql2);
            pstmt[2] = conn.prepareStatement(sql3);
            pstmt[3] = conn.prepareStatement(sql4);
            pstmt[4] = conn.prepareStatement(sql5);

            // get loanapps info
            rs = pstmt[0].executeQuery();
            if(rs.next()) {
                appid = rs.getInt(1);
                applicantID=rs.getInt(2);
                amount = rs.getInt(3);
                duration =rs.getInt(4);
                appts = rs.getTimestamp(5);
            }
            rs.close();

            if(status=="accept") {
                Date date=null;
                if(appts.getTime()>CR.endDate.getTime())
                    date = rg.getRandomTimestamp(CR.endDate.getTime(),appts.getTime());
                else
                    date = rg.getRandomTimestamp(appts.getTime(), CR.endDate.getTime());
                contract_ts = new Timestamp(date.getTime());

                // Insert into the LOANTRANS
                pstmt[1].setInt(1, applicantID);
                pstmt[1].setInt(2, appid);
                pstmt[1].setDouble(3, amount);
                pstmt[1].setString(4, status);
                pstmt[1].setTimestamp(5, appts);
                pstmt[1].setInt(6, duration);
                pstmt[1].setTimestamp(7, contract_ts);
                pstmt[1].setInt(8, 0);
                pstmt[1].execute();
            }
            else{
                // Update the balance
                if(applicantID < customer_no) {
                    pstmt[2].setInt(1, amount);
                    pstmt[2].setInt(2, applicantID);
                    pstmt[2].executeUpdate();
                }
                else {
                    pstmt[3].setInt(1, amount);
                    pstmt[3].setInt(2, applicantID);
                    pstmt[3].executeUpdate();
                }
            }

            // update loanapps status
            pstmt[4].setString(1, status);
            pstmt[4].setInt(2, appid);
            pstmt[4].executeUpdate();
            conn.commit();
            long currentEndTs = System.currentTimeMillis();
            responseTime = currentEndTs - currentStarttTs;
            hist.getTPItem(12).addValue(responseTime);
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
                if(pstmt[0] != null)
                    pstmt[0].close();
                if(pstmt[1] != null)
                    pstmt[1].close();
                if(pstmt[2] != null)
                    pstmt[2].close();
                if(pstmt[3] != null)
                    pstmt[3].close();
                if(pstmt[4] != null)
                    pstmt[4].close();


            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return cr;
    }

    public ClientResult execTxn14(Connection conn) {
        ClientResult cr = new ClientResult();
        PreparedStatement pstmt[] = new PreparedStatement[3];
        ResultSet rs = null;
        long responseTime = 0L;
        int id = 0;
        int applicantid =0;
        double amount = 0;
        java.sql.Timestamp contract_ts = null;
        java.sql.Timestamp current_ts = null;

        try {
            String[] statements= sqls.tp_txn14();
            String sql1=statements[0];
            String sql2=statements[1];
            String sql3=statements[2];
            long currentStarttTs = System.currentTimeMillis();
            // transaction begins
            conn.setAutoCommit(false);

            pstmt[0]=conn.prepareStatement(sql1);
            rs= pstmt[0].executeQuery();
            if (rs.next()){
                id = rs.getInt(1);
                applicantid = rs.getInt(2);
                amount = rs.getDouble(3);
                contract_ts =rs.getTimestamp(4);
                Date date = CR.endDate;
                if(contract_ts!=null){
                    if(contract_ts.getTime()>CR.endDate.getTime())
                        date = rg.getRandomTimestamp(CR.endDate.getTime(),contract_ts.getTime());
                    else
                        date = rg.getRandomTimestamp(contract_ts.getTime(), CR.endDate.getTime());
                }

                current_ts = new Timestamp(date.getTime());
            }
            rs.close();
            pstmt[1]=conn.prepareStatement(sql2);
            pstmt[1].setDouble(1,amount);
            pstmt[1].setInt(2, applicantid);
            pstmt[1].executeUpdate();
            pstmt[2]=conn.prepareStatement(sql3);
            pstmt[2].setTimestamp(1,current_ts);
            pstmt[2].setInt(2,id);
            pstmt[2].executeUpdate();
            conn.commit();
            long currentEndTs = System.currentTimeMillis();
            responseTime = currentEndTs - currentStarttTs;
            hist.getTPItem(13).addValue(responseTime);
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
                if(pstmt[0] != null)
                    pstmt[0].close();
                if(pstmt[1] != null)
                    pstmt[1].close();
                if(pstmt[2] != null)
                    pstmt[2].close();

            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return cr;
    }

    public ClientResult execTxn15(Connection conn) {
        ClientResult cr = new ClientResult();
        PreparedStatement pstmt[] = new PreparedStatement[2];
        ResultSet rs = null;
        long current_date = CR.endDate.getTime();
        java.sql.Timestamp current_ts = new Timestamp(CR.endDate.getTime());
        long responseTime = 0L;
        try {
            String[] statements= sqls.tp_txn15();
            String sql1=statements[0];
            String sql2=statements[1];
            int id=0;
            int applicantID = 0;
            int duration =0;
            java.sql.Timestamp contract_ts = null;

            long currentStarttTs = System.currentTimeMillis();

            // transaction begins
            conn.setAutoCommit(false);
            pstmt[0] = conn.prepareStatement(sql1);
            pstmt[1] = conn.prepareStatement(sql2);

            // get loantrans info
            rs = pstmt[0].executeQuery();
            if(rs.next()) {
                id = rs.getInt(1);
                applicantID=rs.getInt(2);
                duration =rs.getInt(3);
                contract_ts = rs.getTimestamp(4);
            }
            rs.close();
            Long duration_ts = Long.valueOf(duration * 24 * 60 * 60 * 1000);
            // check if it is overdued
            if(contract_ts != null && duration_ts + contract_ts.getTime() < current_date) {
                // update the LOANTRANS
                pstmt[1].setTimestamp(1,current_ts);
                pstmt[1].setInt(2,id);
                pstmt[1].executeUpdate();
            }
            conn.commit();
            long currentEndTs = System.currentTimeMillis();
            responseTime = currentEndTs - currentStarttTs;
            hist.getTPItem(14).addValue(responseTime);
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
                if(pstmt[0] != null)
                    pstmt[0].close();
                if(pstmt[1] != null)
                    pstmt[1].close();

            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return cr;
    }

    public ClientResult execTxn16(Connection conn) {
        ClientResult cr = new ClientResult();
        PreparedStatement pstmt[] = new PreparedStatement[4];;
        ResultSet rs = null;
        long responseTime = 0L;
        java.sql.Timestamp current_ts = null;
        java.sql.Timestamp ts = null;
        try {
            String[] statements= sqls.tp_txn16();
            String sql1=statements[0];
            String sql2=statements[1];
            String sql3=statements[2];
            String sql4=statements[3];
            int id=0;
            int applicantID = 0;
            double amount = 0;
            double balance = 0;
            int duration =0;
            Date date = null;
            long currentStarttTs = System.currentTimeMillis();
            // transaction begins
            conn.setAutoCommit(false);
            pstmt[0] = conn.prepareStatement(sql1);
            pstmt[1] = conn.prepareStatement(sql2);
            pstmt[2] = conn.prepareStatement(sql3);
            pstmt[3] = conn.prepareStatement(sql4);
            // get loantrans info
            rs = pstmt[0].executeQuery();
            while(rs.next()) {
                id = rs.getInt(1);
                applicantID = rs.getInt(2);
                duration = rs.getInt(3);
                amount = rs.getDouble(4);
                ts = rs.getTimestamp(5);
                if(ts != null && ts.getTime()<CR.endDate.getTime())
                    date = rg.getRandomTimestamp(ts.getTime(),CR.endDate.getTime());
                else
                    date = CR.endDate;
                current_ts = new Timestamp(date.getTime());
                break;
            }
            rs.close();
            // get the balance
            pstmt[1].setInt(1, applicantID);
            rs = pstmt[1].executeQuery();
            while(rs.next()) {
                balance = rs.getDouble(1);
                break;
            }
            rs.close();
            if (balance < amount){
                conn.rollback();
            }
            else{
                // update the balance
                pstmt[2].setDouble(1, amount);
                pstmt[2].setInt(2, applicantID);
                pstmt[2].executeUpdate();

                // update the loan status
                pstmt[3].setTimestamp(1,current_ts);
                pstmt[3].setInt(2,id);
                pstmt[3].executeUpdate();
            }
            conn.commit();
            long currentEndTs = System.currentTimeMillis();
            responseTime = currentEndTs - currentStarttTs;
            hist.getTPItem(15).addValue(responseTime);
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
                if(pstmt[0] != null)
                    pstmt[0].close();
                if(pstmt[1] != null)
                    pstmt[1].close();
                if(pstmt[2] != null)
                    pstmt[2].close();
                if(pstmt[3] != null)
                    pstmt[3].close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return cr;
    }

    public ClientResult execTxn17(Connection conn) {
        ClientResult cr = new ClientResult();
        // Get random accountid
        int accountid = rg.getRandomint(1,customer_no+company_no);

        String type =null;
        double amount = 0;
        if(accountid<customer_no){
            type = rg.getRandomCustTransferType();
            amount = rg.getRandomDouble(CR.customer_savingbalance * 0.0001);
        }
        else{
            type = rg.getRandomCompanyTransferType();
            amount = rg.getRandomDouble(CR.company_savingbalance * 0.0001);
        }

        PreparedStatement pstmt[] = new PreparedStatement[4];
        ResultSet rs = null;

        long responseTime = 0L;

        try {
            String[] statements= sqls.tp_txn17();

            String sql1=statements[0];
            String sql2=statements[1];
            String sql3=statements[2];
            String sql4=statements[3];
            long currentStarttTs = System.currentTimeMillis();
            // transaction begins
            conn.setAutoCommit(false);
            double balance =0;
            int isBlocked = 0;
            pstmt[0] = conn.prepareStatement(sql1);
            pstmt[1]= conn.prepareStatement(sql2);
            pstmt[2]= conn.prepareStatement(sql3);
            pstmt[3]= conn.prepareStatement(sql4);

            pstmt[0].setInt(1,accountid);
            rs= pstmt[0].executeQuery();
            if (rs.next()){
                balance = rs.getDouble(1);
                isBlocked = rs.getInt(2);
            }
            rs.close();

            if (balance < amount || isBlocked == 1){ // Check the balance
                conn.rollback();
            }
            else
            {
                pstmt[0].setInt(1,accountid);
                rs= pstmt[0].executeQuery();
                if (rs.next()){
                    isBlocked = rs.getInt(1);
                }
                rs.close();

                if ( isBlocked == 1){ // Check checking account is blocked or not
                    conn.rollback();
                }
                else
                {
                    // update the savingaccount balance
                    pstmt[2].setDouble(1, amount);
                    pstmt[2].setInt(2,accountid);
                    pstmt[2].executeUpdate();

                    // update the checkingaccount balance
                    pstmt[3].setDouble(1, amount);
                    pstmt[3].setInt(2,accountid);
                    pstmt[3].executeUpdate();
                    conn.commit();
                }

            }
            long currentEndTs = System.currentTimeMillis();
            responseTime = currentEndTs - currentStarttTs;
            hist.getTPItem(16).addValue(responseTime);
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
                pstmt[0].close();
                pstmt[1].close();
                pstmt[2].close();
                pstmt[3].close();

            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return cr;
    }

    public ClientResult execTxn18(Connection conn) {
        ClientResult cr = new ClientResult();
        // Get random accountid
        int accountid = rg.getRandomint(1,customer_no+company_no);

        String type =null;
        double amount = 0;
        if(accountid<customer_no){
            type = rg.getRandomCustTransferType();
            amount = rg.getRandomDouble(CR.customer_savingbalance * 0.0001);
        }
        else{
            type = rg.getRandomCompanyTransferType();
            amount = rg.getRandomDouble(CR.company_savingbalance * 0.0001);
        }

        PreparedStatement pstmt[] = new PreparedStatement[4];
        ResultSet rs = null;

        long responseTime = 0L;

        try {
            String[] statements= sqls.tp_txn18();

            String sql1=statements[0];
            String sql2=statements[1];
            String sql3=statements[2];
            String sql4=statements[3];
            long currentStarttTs = System.currentTimeMillis();
            // transaction begins
            conn.setAutoCommit(false);
            double balance =0;
            int isBlocked = 0;
            pstmt[0] = conn.prepareStatement(sql1);
            pstmt[1]= conn.prepareStatement(sql2);
            pstmt[2]= conn.prepareStatement(sql3);
            pstmt[3]= conn.prepareStatement(sql4);

            pstmt[0].setInt(1,accountid);
            rs= pstmt[0].executeQuery();
            if (rs.next()){
                balance = rs.getDouble(1);
                isBlocked = rs.getInt(2);
            }
            rs.close();

            if (balance < amount || isBlocked == 1){ // Check the balance
                conn.rollback();
            }
            else
            {
                pstmt[0].setInt(1,accountid);
                rs= pstmt[0].executeQuery();
                if (rs.next()){
                    isBlocked = rs.getInt(1);
                }
                rs.close();

                if ( isBlocked == 1){ // Check checking account is blocked or not
                    conn.rollback();
                }
                else
                {
                    // update the savingaccount balance
                    pstmt[2].setDouble(1, amount);
                    pstmt[2].setInt(2,accountid);
                    pstmt[2].executeUpdate();

                    // update the checkingaccount balance
                    pstmt[3].setDouble(1, amount);
                    pstmt[3].setInt(2,accountid);
                    pstmt[3].executeUpdate();
                    conn.commit();
                }

            }
            long currentEndTs = System.currentTimeMillis();
            responseTime = currentEndTs - currentStarttTs;
            hist.getTPItem(17).addValue(responseTime);
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
                pstmt[0].close();
                pstmt[1].close();
                pstmt[2].close();
                pstmt[3].close();

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
        long totalElapsedTime = 0L;
        try {
            Class<CloudTPClient> tpClass = (Class<CloudTPClient>)Class.forName("com.hybench.workload.CloudTPClient");
            if(type == 8){

                while(!exitFlag) {
                    int rand = ThreadLocalRandom.current().nextInt(1, 100);
                    if(rand < tp1_percent){
                        cr = execTxn1(conn);
                    }
                    else if(rand < tp1_percent + tp2_percent){
                        cr = execTxn2(conn);
                    }
                    else if(rand < tp1_percent + tp2_percent+tp3_percent){
                        cr = execTxn3(conn);
                    }
                    else if(rand < tp1_percent + tp2_percent+tp3_percent+tp4_percent){
                        cr = execTxn4(conn);
                    }
                    else if(rand < tp1_percent + tp2_percent+tp3_percent+tp4_percent+tp5_percent){
                        cr = execTxn5(conn);
                    }
                    else if(rand < tp1_percent + tp2_percent+tp3_percent+tp4_percent+tp5_percent
                            +tp6_percent){
                        cr = execTxn6(conn);
                    }
                    else if(rand < tp1_percent + tp2_percent+tp3_percent+tp4_percent+tp5_percent
                            +tp6_percent+tp7_percent){
                        cr = execTxn7(conn);
                    }
                    else if(rand < tp1_percent + tp2_percent+tp3_percent+tp4_percent+tp5_percent
                            +tp6_percent+tp7_percent+tp8_percent){
                        cr = execTxn8(conn);
                    }
                    else if(rand < tp1_percent + tp2_percent+tp3_percent+tp4_percent+tp5_percent
                            +tp6_percent+tp7_percent+tp8_percent+tp9_percent){
                        cr = execTxn9(conn);
                    }
                    else if(rand < tp1_percent + tp2_percent+tp3_percent+tp4_percent+tp5_percent
                            +tp6_percent+tp7_percent+tp8_percent+tp9_percent+tp10_percent){
                        cr = execTxn10(conn);
                    }
                    else if(rand < tp1_percent + tp2_percent+tp3_percent+tp4_percent+tp5_percent
                            +tp6_percent+tp7_percent+tp8_percent+tp9_percent+tp10_percent
                            +tp11_percent){
                        cr = execTxn11(conn);
                    }
                    else if(rand < tp1_percent + tp2_percent+tp3_percent+tp4_percent+tp5_percent
                            +tp6_percent+tp7_percent+tp8_percent+tp9_percent+tp10_percent
                            +tp11_percent+tp12_percent){
                        cr = execTxn12(conn);
                    }
                    else if(rand < tp1_percent + tp2_percent+tp3_percent+tp4_percent+tp5_percent
                            +tp6_percent+tp7_percent+tp8_percent+tp9_percent+tp10_percent
                            +tp11_percent+tp12_percent+tp13_percent){
                        cr = execTxn13(conn);
                    }
                    else if(rand < tp1_percent + tp2_percent+tp3_percent+tp4_percent+tp5_percent
                            +tp6_percent+tp7_percent+tp8_percent+tp9_percent+tp10_percent
                            +tp11_percent+tp12_percent+tp13_percent+tp14_percent){
                        cr = execTxn14(conn);
                    }
                    else if(rand < tp1_percent + tp2_percent+tp3_percent+tp4_percent+tp5_percent
                            +tp6_percent+tp7_percent+tp8_percent+tp9_percent+tp10_percent
                            +tp11_percent+tp12_percent+tp13_percent+tp14_percent+tp15_percent){
                        cr = execTxn15(conn);
                    }
                    else if(rand < tp1_percent + tp2_percent+tp3_percent+tp4_percent+tp5_percent
                            +tp6_percent+tp7_percent+tp8_percent+tp9_percent+tp10_percent
                            +tp11_percent+tp12_percent+tp13_percent+tp14_percent+tp15_percent
                            +tp16_percent){
                        cr = execTxn16(conn);
                    }
                    else if(rand < tp1_percent + tp2_percent+tp3_percent+tp4_percent+tp5_percent
                            +tp6_percent+tp7_percent+tp8_percent+tp9_percent+tp10_percent
                            +tp11_percent+tp12_percent+tp13_percent+tp14_percent+tp15_percent
                            +tp16_percent+tp17_percent){
                        cr = execTxn17(conn);
                    }
                    else {
                        cr = execTxn18(conn);
                    }
                    totalElapsedTime += cr.getRt();
                    if(exitFlag)
                        break;
                }
                ret.setRt(totalElapsedTime);
            }
            else if(type == 0 || type == 4){
                while(!exitFlag){
                    int rand = ThreadLocalRandom.current().nextInt(1, 100);
                    if(type == 4 && Thread.currentThread().getName().equalsIgnoreCase("T1")){
//                        if(rand < fresh_percent){
//                            cr= execFresh3(conn);
//                        }
                        if(rand < fresh_percent/2){
                            cr= execFresh2(conn);
                        }
                        else if(rand < fresh_percent){
                            cr= execFresh(conn);
                        }
                        else
                            continue;
                    }
                    else{
                        if(rand < at1_percent){
                            cr = execAT1(conn);
                        }
                        else if(rand < at1_percent + at2_percent){
                            cr = execAT2(conn);
                        }
                        else if(rand < at1_percent + at2_percent+at3_percent){
                            cr = execAT3(conn);
                        }
                        else if(rand < at1_percent + at2_percent+at3_percent+at4_percent){
                            cr = execAT4(conn);
                        }
                        else if(rand < at1_percent + at2_percent+at3_percent+at4_percent+at5_percent){
                            cr = execAT5(conn);
                        }
                        else {
                            cr = execAT6(conn);
                        }
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
