package com.cloudybench.workload;

/**
 *
 * @version 1.00
 * @time 2023-03-07
 * @file SqlReader.java
 **/

import com.moandjiezana.toml.Toml;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.List;


public class SqlReader {
    String filePath = null;
    Sqlstmts sqls = null;


    public SqlReader(String filePath){
        this.filePath = filePath;
    }
    // read sqls from sql files
    public Sqlstmts loader(){
        sqls = new Sqlstmts();
        try{
            BufferedReader Br = new BufferedReader(new FileReader(filePath));
            Toml toml = new Toml().read(Br);

            sqls.setTp_txn1(getSqlArrayFromList(toml.getList("TP-1.sql")));
            sqls.setTp_txn2(getSqlArrayFromList(toml.getList("TP-2.sql")));
            sqls.setTp_txn3(toml.getString("TP-3.sql"));
            sqls.setTp_txn4(getSqlArrayFromList(toml.getList("TP-4.sql")));

        }catch(Exception e){
            e.printStackTrace();
        }
        return sqls;
    }

    public String[] getSqlArrayFromList(List<String> sqlList){
        String[] sqls = new String[sqlList.size()];
        for(int i = 0;i < sqlList.size();i++){
            sqls[i] = sqlList.get(i);
        }
        return sqls;
    }

    public Sqlstmts getSqls(){
        return this.sqls;
    }
}
