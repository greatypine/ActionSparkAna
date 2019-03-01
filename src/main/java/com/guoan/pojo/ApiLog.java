package com.guoan.pojo;

import java.io.Serializable;

public class ApiLog implements Serializable ,Cloneable{
	
	private static final long serialVersionUID = -4103145769676229347L;
	
	private String id ;
	private String requestUri;
	private String store_id; 
	private String createTime; 
	private String createDate;
	private String token;
	private String mdop;
	private String adtag;
	private String appTypePlatform;
	//json格式的字符串
	private String group_id;
	private String order_id;
	private String message;
	private String payPlatform;
	private String phone ;
	//员工角色
	private String employee_role;
		
	
	
	
	public String getStore_id() {
		return store_id;
	}
	public void setStore_id(String store_id) {
		this.store_id = store_id;
	}
	public String getGroup_id() {
		return group_id;
	}
	public void setGroup_id(String group_id) {
		this.group_id = group_id;
	}
	public String getOrder_id() {
		return order_id;
	}
	public void setOrder_id(String order_id) {
		this.order_id = order_id;
	}
	public String getPhone() {
		return phone;
	}
	public void setPhone(String phone) {
		this.phone = phone;
	}
	public String getEmployee_role() {
		return employee_role;
	}
	public void setEmployee_role(String employee_role) {
		this.employee_role = employee_role;
	}
	public String getPayPlatform() {
		return payPlatform;
	}
	public void setPayPlatform(String payPlatform) {
		this.payPlatform = payPlatform;
	}
	public String getMdop() {
		return mdop;
	}
	public void setMdop(String mdop) {
		this.mdop = mdop;
	}
	public String getAdtag() {
		return adtag;
	}
	public void setAdtag(String adtag) {
		this.adtag = adtag;
	}
	public String getMessage() {
		return message;
	}
	public void setMessage(String message) {
		this.message = message;
	}
	public String getCreateDate() {
		return createDate;
	}
	public void setCreateDate(String createDate) {
		this.createDate = createDate;
	}
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getRequestUri() {
		return requestUri;
	}
	public void setRequestUri(String requestUri) {
		this.requestUri = requestUri;
	}
	public String getCreateTime() {
		return createTime;
	}
	public void setCreateTime(String createTime) {
		this.createTime = createTime;
	}
	public String getToken() {
		return token;
	}
	public void setToken(String token) {
		this.token = token;
	}
	public String getAppTypePlatform() {
		return appTypePlatform;
	}
	public void setAppTypePlatform(String appTypePlatform) {
		this.appTypePlatform = appTypePlatform;
	}
	
	 @Override  
    public Object clone() throws CloneNotSupportedException  
    {  
        return super.clone();  
    }

}
