package com.cloudybench.workload;
/**
    *
    * @time 2023-03-04
    * @version 1.0.0
    * @file ClientResult.java
    * @description
    *   client result is recorded in this class
 **/


public class ClientResult {
    private boolean succeed = true;
    private String errorCode = "000000";
    private String errorMsg = "";
    private Throwable exception = null;
    public final static ClientResult SUCCEED = new ClientResult();
    public final static ClientResult FAILED = new ClientResult(false,"-1","UNKNOWN REASON");
    private double rt = 0L;
    private int apRound = 0;

    public ClientResult(){

    }

    public ClientResult(boolean res, String errCode, String errMsg){
        this.succeed = res;
        this.errorCode = errCode;
        this.errorMsg = errMsg;
    }

    public int getApRound() {
        return apRound;
    }

    public void setApRound(int apRound) {
        this.apRound = apRound;
    }

    public double getRt(){
        return rt;
    }

    public void setRt(double rt){
        this.rt = rt;
    }

    public ClientResult(boolean res, Throwable excep){
        this(res,"-1","unknown");
        this.exception = excep;
    }

    public ClientResult(boolean res, String errCode, Throwable excep){
        this(res, errCode, "UNKNOWN");
        this.exception = excep;
    }

    public ClientResult(boolean res, String errCode, String errMsg, Throwable excep){
        this(res,errCode,errMsg);
        this.exception = excep;
    }

    public boolean isSucceed(){
        return succeed;
    }

    public void setResult(boolean ret){
        this.succeed = ret;
    }

    public void setErrorMsg(String errorMsg){
        this.errorMsg = errorMsg;
    }

    public void setErrorCode(String errorCode){
        this.errorCode = errorCode;
    }

}
