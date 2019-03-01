package com.guoan.pojo;

import java.io.Serializable;
import java.util.List;

/**
  * Description: log日志类,es数据源  
  * @author lyy  
  * @date 2018年9月4日
 */
public class Log_Search implements Serializable ,Cloneable{

	private static final long serialVersionUID = 1759119112402796323L;
	private String id;
	private String customer_id;
	private String product_id;
	private String product_name;
	private String key;
	private String create_time;
	private String store_id;
	//前置仓
	private String front_store_id ;
	//云门店
	private String cloud_store_id;
	private String mobilephone;
	private String simple_date;
    
	
	
	
	public String getFront_store_id() {
		return front_store_id;
	}
	public void setFront_store_id(String front_store_id) {
		this.front_store_id = front_store_id;
	}
	public String getCloud_store_id() {
		return cloud_store_id;
	}
	public void setCloud_store_id(String cloud_store_id) {
		this.cloud_store_id = cloud_store_id;
	}
	public String getProduct_name() {
		return product_name;
	}
	public void setProduct_name(String product_name) {
		this.product_name = product_name;
	}
	public String getSimple_date() {
		return simple_date;
	}
	public void setSimple_date(String simple_date) {
		this.simple_date = simple_date;
	}
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
	
	public String getProduct_id() {
		return product_id;
	}
	public void setProduct_id(String product_id) {
		this.product_id = product_id;
	}
	public String getKey() {
		return key;
	}
	public void setKey(String key) {
		this.key = key;
	}
	public String getCreate_time() {
		return create_time;
	}
	public void setCreate_time(String create_time) {
		this.create_time = create_time;
	}
	public String getStore_id() {
		return store_id;
	}
	public void setStore_id(String store_id) {
		this.store_id = store_id;
	}
	public String getMobilephone() {
		return mobilephone;
	}
	public void setMobilephone(String mobilephone) {
		this.mobilephone = mobilephone;
	}
	
	 @Override  
    public Object clone() throws CloneNotSupportedException  
    {  
        return super.clone();  
    }
	
	
	
}
