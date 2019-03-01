package com.guoan.utils;


import java.io.Serializable;
import java.util.List;

/**
 * Created by gad on 2017/11/27.
 */
public class ReturnResult implements Serializable{


	private static final long serialVersionUID = -424444132885180373L;

	// 200------success-成功查询结果
    //-100------error-参数错误：页面size(limit)必须>=1且offset必须>=0
    //-101------error-获取通往impala连接失败：请检查impala集群是否运行正常，网络是否通畅！
    //-102-----"error-sql执行异常：SQLException:"+e.getMessage().substring(0,100)
    //-103------error-资源关闭异常：ResultSet／PreparedStatement／Connection未能正确关闭！
    private int returnCode;

    //提示信息
    private String msg;

    //数据总量
    private long total;

    //结果集合
    private List<?> rows;

    public ReturnResult() {
        returnCode = 200;
        msg = "success-成功查询结果";
    }

    public int getReturnCode() {
        return returnCode;
    }

    public void setReturnCode(int returnCode) {
        this.returnCode = returnCode;
    }

    public long getTotal() {
        return total;
    }

    public void setTotal(long total) {
        this.total = total;
    }

    public List<?> getRows() {
        return rows;
    }

    public void setRows(List<?> rows) {
        this.rows = rows;
    }


    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }
}
