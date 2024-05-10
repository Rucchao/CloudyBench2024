package com.cloudybench.dbconn;
/**
 *
 * @time 2023-03-04
 * @version 1.0.0
 * @file ConnectionMgr.java
 * @description
 *   use to get db connection
 **/

import com.cloudybench.ConfigLoader;
import com.cloudybench.util.RandomGenerator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

public class ConnectionMgr {
   static Logger logger = LogManager.getLogger(ConnectionMgr.class);
    // get connection from default url
    public static Connection getConnection(){
        Connection conn = null;
        Properties prop = new Properties();
        try {
            FileInputStream fis = new FileInputStream(ConfigLoader.confFile);
            prop.load(fis);
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            logger.error("Read configure failed : " + ConfigLoader.confFile,e  );
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            logger.error("Read configure failed : " + ConfigLoader.confFile,e);
            e.printStackTrace();
        }

        try {
            Class.forName(prop.getProperty("classname"));
            //DriverManager.registerDriver(new oracle.jdbc.OracleDriver());
            conn = DriverManager.getConnection(
                    prop.getProperty("url"),
                    prop.getProperty("username"),
                    prop.getProperty("password"));
            conn.setAutoCommit(false);
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            logger.error( "Getting connection failed! " + prop.getProperty("url")+" : " + prop.getProperty("username")+" : " + prop.getProperty("password"));
            e.printStackTrace();
        }
        catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return conn;
    }

    public static Connection getReplicaConnection(){
        Connection conn = null;
        Properties prop = new Properties();
        try {
            FileInputStream fis = new FileInputStream(ConfigLoader.confFile);
            prop.load(fis);
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            logger.error("Read configure failed : " + ConfigLoader.confFile,e  );
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            logger.error("Read configure failed : " + ConfigLoader.confFile,e);
            e.printStackTrace();
        }

        try {
            Class.forName(prop.getProperty("classname_replica"));
            //DriverManager.registerDriver(new oracle.jdbc.OracleDriver());

            // get a random replica url
            //double rand = rg.getRandomDouble();
            int rand = ThreadLocalRandom.current().nextInt(1, 100);
            if(rand<40){
                conn = DriverManager.getConnection(
                        prop.getProperty("url_replica_1"),
                        prop.getProperty("username_replica"),
                        prop.getProperty("password_replica"));
               // System.out.println("The replica url is url_replica_1!");

            }
            else if(rand<70){
                conn = DriverManager.getConnection(
                        prop.getProperty("url_replica_2"),
                        prop.getProperty("username_replica"),
                        prop.getProperty("password_replica"));
               // System.out.println("The replica url is url_replica_2!");
            }
            else if (rand<100){
                conn = DriverManager.getConnection(
                        prop.getProperty("url_replica_3"),
                        prop.getProperty("username_replica"),
                        prop.getProperty("password_replica"));
              //  System.out.println("The replica url is url_replica_3!");
            }

            conn.setAutoCommit(false);
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            logger.error( "Getting connection failed! " + prop.getProperty("url")+" : " + prop.getProperty("username")+" : " + prop.getProperty("password"));
            e.printStackTrace();
        }
        catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return conn;
    }

    // type = 0 means get connection from tp url and type =1 means get conneciton from ap url
    public static Connection getConnection(int type){
        Connection conn = null;
        Properties prop = new Properties();
        try {
            FileInputStream fis = new FileInputStream(ConfigLoader.confFile);
            prop.load(fis);
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            logger.error("Read configure failed : " + ConfigLoader.confFile,e  );
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            logger.error("Read configure failed : " + ConfigLoader.confFile,e);
            e.printStackTrace();
        }
        String url = prop.getProperty("url");
        String username = prop.getProperty("username");
        String password = prop.getProperty("password");
        try {
            if( 1 == type){
                Class.forName(prop.getProperty("classname_ap"));
                url = prop.getProperty("url_ap");
                username = prop.getProperty("username_ap");
                password = prop.getProperty("password_ap");
            }
            else
                Class.forName(prop.getProperty("classname"));
            conn = DriverManager.getConnection(
                    url,
                    username,
                    password);
            conn.setAutoCommit(false);
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            logger.error( "Getting connection failed! " + url +" : " + username +" : " + password );
            e.printStackTrace();
        }
        catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return conn;
    }

    // True refers to multi-tenancy mode
    public static Connection getConnection(int tenant_number, boolean True){
        Connection conn = null;
        Properties prop = new Properties();
        try {
            FileInputStream fis = new FileInputStream(ConfigLoader.confFile);
            prop.load(fis);
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            logger.error("Read configure failed : " + ConfigLoader.confFile,e  );
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            logger.error("Read configure failed : " + ConfigLoader.confFile,e);
            e.printStackTrace();
        }
        String url = prop.getProperty("url");
        String username = prop.getProperty("username");
        String password = prop.getProperty("password");
        try {
            Class.forName(prop.getProperty("classname"));
            url = prop.getProperty("url_"+tenant_number);
            username = prop.getProperty("username_"+tenant_number);
            password = prop.getProperty("password");
            conn = DriverManager.getConnection(
                    url,
                    username,
                    password);
            conn.setAutoCommit(false);
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            logger.error( "Getting connection failed! " + url +" : " + username +" : " + password );
            e.printStackTrace();
        }
        catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return conn;
    }

}
