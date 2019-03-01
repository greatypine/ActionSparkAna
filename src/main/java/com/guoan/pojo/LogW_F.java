package com.guoan.pojo;

import java.io.Serializable;
/**
  * Description:  微信端日志封装类  map对照表
  * @author lyy  
  * @date 2018年6月7日
 */
public class LogW_F implements Serializable ,Cloneable{

	private static final long serialVersionUID = 5229831337103256370L;
	
	private String customer;
	private String mdop;
	
	public String getCustomer() {
		return customer;
	}
	public void setCustomer(String customer) {
		this.customer = customer;
	}
	public String getMdop() {
		return mdop;
	}
	public void setMdop(String mdop) {
		this.mdop = mdop;
	}
	
	
}
