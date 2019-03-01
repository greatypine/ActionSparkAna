package com.guoan.pojo;

import java.io.Serializable;

public class CustomerOrders implements Serializable {

	private static final long serialVersionUID = -7862456456765257101L;
	private String customer_id;
	private String times;
	
	
	
	public String getCustomer_id() {
		return customer_id;
	}
	public void setCustomer_id(String customer_id) {
		this.customer_id = customer_id;
	}
	public String getTimes() {
		return times;
	}
	public void setTimes(String times) {
		this.times = times;
	}
}

