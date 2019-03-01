package com.guoan.pojo;

import java.io.Serializable;

/**
  * Description:  日志汇总的中间表 
  * @author lyy  
  * @date 2018年6月20日
 */
public class Log_Collect implements Serializable{
	private static final long serialVersionUID = -8274192774268052297L;
	
	private String id;
	private String token;
	private String customer_id;
	private String phone ;
	private String behavior;
	private String behavior_param;
	private String behavior_name;
	private String create_date;
	//创建一个yyyy-mm-dd 日期,方便后期的表分区
	private String simple_date;
	private String two_behavior;
	private String two_behavior_name;
	private String store_id;
	private String eshop_id;
	private String message;
	private String payPlatform;
	private String order_id;
	private String group_id;
	private String log_type;
	//员工角色
	private String employee_role;
	//商品id
	//E店id
	private String product_id;
	
	
	public String getProduct_id() {
		return product_id;
	}
	public void setProduct_id(String product_id) {
		this.product_id = product_id;
	}
	public String getSimple_date() {
		return simple_date;
	}
	public void setSimple_date(String simple_date) {
		this.simple_date = simple_date;
	}
	public String getEmployee_role() {
		return employee_role;
	}
	public void setEmployee_role(String employee_role) {
		this.employee_role = employee_role;
	}
	public String getPhone() {
		return phone;
	}
	public void setPhone(String phone) {
		this.phone = phone;
	}
	public String getToken() {
		return token;
	}
	public void setToken(String token) {
		this.token = token;
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
	public String getBehavior() {
		return behavior;
	}
	public void setBehavior(String behavior) {
		this.behavior = behavior;
	}
	public String getBehavior_param() {
		return behavior_param;
	}
	public void setBehavior_param(String behavior_param) {
		this.behavior_param = behavior_param;
	}
	public String getBehavior_name() {
		return behavior_name;
	}
	public void setBehavior_name(String behavior_name) {
		this.behavior_name = behavior_name;
	}
	public String getCreate_date() {
		return create_date;
	}
	public void setCreate_date(String create_date) {
		this.create_date = create_date;
	}
	public String getTwo_behavior() {
		return two_behavior;
	}
	public void setTwo_behavior(String two_behavior) {
		this.two_behavior = two_behavior;
	}
	public String getTwo_behavior_name() {
		return two_behavior_name;
	}
	public void setTwo_behavior_name(String two_behavior_name) {
		this.two_behavior_name = two_behavior_name;
	}
	public String getStore_id() {
		return store_id;
	}
	public void setStore_id(String store_id) {
		this.store_id = store_id;
	}
	public String getEshop_id() {
		return eshop_id;
	}
	public void setEshop_id(String eshop_id) {
		this.eshop_id = eshop_id;
	}
	public String getMessage() {
		return message;
	}
	public void setMessage(String message) {
		this.message = message;
	}
	public String getPayPlatform() {
		return payPlatform;
	}
	public void setPayPlatform(String payPlatform) {
		this.payPlatform = payPlatform;
	}
	public String getOrder_id() {
		return order_id;
	}
	public void setOrder_id(String order_id) {
		this.order_id = order_id;
	}
	public String getGroup_id() {
		return group_id;
	}
	public void setGroup_id(String group_id) {
		this.group_id = group_id;
	}
	public String getLog_type() {
		return log_type;
	}
	public void setLog_type(String log_type) {
		this.log_type = log_type;
	}
	
	

}
