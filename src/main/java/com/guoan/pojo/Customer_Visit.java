package com.guoan.pojo;

import java.io.Serializable;
/**
  * Description: 用户浏览类
  * @author lyy  
  * @date 2018年8月3日
 */
public class Customer_Visit implements Serializable{

	private static final long serialVersionUID = -2908848727282809226L;
	
	private String id ;
	private String customer_id ;
	private Integer visit_num ; //近30浏览次数
	private String last_login ;//最后一次登陆时间
	
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getCustomer_id() {
		return customer_id;
	}
	public void setCustomer_id(String customer_id) {
		this.customer_id = customer_id;
	}
	public Integer getVisit_num() {
		return visit_num;
	}
	public void setVisit_num(Integer visit_num) {
		this.visit_num = visit_num;
	}
	public String getLast_login() {
		return last_login;
	}
	public void setLast_login(String last_login) {
		this.last_login = last_login;
	}
	@Override
	public String toString() {
		return "Customer_Visit [id=" + id + ", customer_id=" + customer_id + ", visit_num=" + visit_num
				+ ", last_login=" + last_login + "]";
	}
	
	
	
}
