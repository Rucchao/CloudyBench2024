package com.cloudybench.workload;

/**
 *
 * @version 1.00
 * @time 2023-03-08
 * @file Sqlstmts.java
 *
 **/
public class Sqlstmts {

    // TP querys total 6
    private  String tp_txn1 = null;
    private  String[] tp_txn2 = null;
    private  String tp_txn3 = null;

    public String tp_txn1() {
        return tp_txn1;
    }

    public void setTp_txn1(String tp_txn1) {
        this.tp_txn1 = tp_txn1;
    }

    public String[] tp_txn2() {
        return tp_txn2;
    }

    public void setTp_txn2(String[] tp_txn2) {
        this.tp_txn2 = tp_txn2;
    }

    public String tp_txn3() {
        return tp_txn3;
    }

    public void setTp_txn3(String tp_txn3) {
        this.tp_txn3 = tp_txn3;
    }

}
