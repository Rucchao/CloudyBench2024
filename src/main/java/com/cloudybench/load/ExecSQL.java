package com.cloudybench.load;
/**
 *
 * @time 2023-03-04
 * @version 1.0.0
 * @file ExecSQL.java
 * @description
 *  execute sqls, such as ddl file
 **/
import com.cloudybench.dbconn.ConnectionMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class ExecSQL {
    Logger logger = LogManager.getLogger(ExecSQL.class);
    String filePath = null;

    public ExecSQL(String file){
        this.filePath = file;
    }

    public void execute(){
        ConnectionMgr cmgr = new ConnectionMgr();
        Connection conn = cmgr.getConnection();
        Statement stmt = null;
        String readLine = null;
        StringBuffer sql = new StringBuffer();
        try {
            conn.setAutoCommit(true);
            stmt = conn.createStatement();
            BufferedReader in = new BufferedReader
                    (new FileReader(filePath));
            while((readLine = in.readLine()) != null) {
                String line = readLine.trim();
                if (line.length() != 0) {
                    if (line.startsWith("--")) {
                        System.out.println(line);
                    } else {
                        if (line.endsWith("\\;"))
                        {
                            sql.append(line.replaceAll("\\\\;", ";"));
                            sql.append("\n");
                        }
                        else
                        {
                            sql.append(line.replaceAll("\\\\;", ";"));
                            if (line.endsWith(";")) {
                                String query = sql.toString();
                                logger.info("execute query:" + query);
                                stmt.execute(query.substring(0, query.length() - 1));
                                sql = new StringBuffer();
                            } else {
                                sql.append("\n");
                            }
                        }
                    }
                }

            }
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally{
            if(conn != null){
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }


    }
}
