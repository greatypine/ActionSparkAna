package com.guoan.pojo;

import java.io.Serializable;
/**
  * Description:  微信端日志封装类 
  * @author lyy  
  * @date 2018年6月7日
 */
public class LogW implements Serializable ,Cloneable{

	private static final long serialVersionUID = -6001321461832159390L;

	private String id;
	private String create_date;
	private String ip;
	private String user_agent;
	private String storeid;
	private String frontid;
	private String phone ;
	private String url ;
	private String route;
	private String customer_id;
	private String mdop;
	private String adtag;
	//商品id
	private String product_id;
	//E店id
	private String eshop_id;
	
	
	public String getEshop_id() {
		return eshop_id;
	}
	public void setEshop_id(String eshop_id) {
		this.eshop_id = eshop_id;
	}
	public String getProduct_id() {
		return product_id;
	}
	public void setProduct_id(String product_id) {
		this.product_id = product_id;
	}
	public String getPhone() {
		return phone;
	}
	public void setPhone(String phone) {
		this.phone = phone;
	}
	public String getCustomer_id() {
		return customer_id;
	}
	public void setCustomer_id(String customer_id) {
		this.customer_id = customer_id;
	}
	public String getUser_agent() {
		return user_agent;
	}
	public void setUser_agent(String user_agent) {
		this.user_agent = user_agent;
	}
	public String getFrontid() {
		return frontid;
	}
	public void setFrontid(String frontid) {
		this.frontid = frontid;
	}
	public String getCreate_date() {
		return create_date;
	}
	public void setCreate_date(String create_date) {
		this.create_date = create_date;
	}
	public String getStoreid() {
		return storeid;
	}
	public void setStoreid(String storeid) {
		this.storeid = storeid;
	}
	public String getRoute() {
		return route;
	}
	public void setRoute(String route) {
		this.route = route;
	}
	public String getIp() {
		return ip;
	}
	public void setIp(String ip) {
		this.ip = ip;
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
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getUrl() {
		return url;
	}
	public void setUrl(String url) {
		this.url = url;
	}
	 @Override  
    public Object clone() throws CloneNotSupportedException  
    {  
        return super.clone();  
    }
	
	
}
