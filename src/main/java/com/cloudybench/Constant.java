package com.cloudybench;

public class Constant {
    final public static int DB_MYSQL = 3;
    final public static int DB_ORACLE = 4;
    final public static int DB_PG = 5;
    final public static int DB_UNKNOW = 0;

    public static int getDbType(String db){

        if(db.equalsIgnoreCase("postgreSQL")){
            return Constant.DB_PG;
        }
        else if(db.equalsIgnoreCase("mysql")){
            return Constant.DB_MYSQL;
        }
        else if(db.equalsIgnoreCase("oracle")){
            return Constant.DB_ORACLE;
        }
        else{
            return Constant.DB_UNKNOW;
        }
    }
}
