package com.guoan.pojo;

import java.io.Serializable;

public class CustomerInfoRecord_save implements Serializable{

	private static final long serialVersionUID = 2701994711676853631L;
	private String id ;
	private String customerid    ;                   
	private String createtime    ;      
	private String phone       ;
	private String inviteCode       ;
	private String citycode ;
	private String idcard      ;        
	private String name   ;
	private String birthday ;
	private String address ;
	private String sex  ;
	private String associatorlevel ;
	private String associatorexpirydate ;
	
	
	
	public String getInviteCode() {
		return inviteCode;
	}
	public void setInviteCode(String inviteCode) {
		this.inviteCode = inviteCode;
	}
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getCustomerid() {
		return customerid;
	}
	public void setCustomerid(String customerid) {
		this.customerid = customerid;
	}
	public String getCreatetime() {
		return createtime;
	}
	public void setCreatetime(String createtime) {
		this.createtime = createtime;
	}
	public String getPhone() {
		return phone;
	}
	public void setPhone(String phone) {
		this.phone = phone;
	}
	public String getCitycode() {
		return citycode;
	}
	public void setCitycode(String citycode) {
		this.citycode = citycode;
	}
	public String getIdcard() {
		return idcard;
	}
	public void setIdcard(String idcard) {
		this.idcard = idcard;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getBirthday() {
		return birthday;
	}
	public void setBirthday(String birthday) {
		this.birthday = birthday;
	}
	public String getAddress() {
		return address;
	}
	public void setAddress(String address) {
		this.address = address;
	}
	public String getSex() {
		return sex;
	}
	public void setSex(String sex) {
		this.sex = sex;
	}
	public String getAssociatorlevel() {
		return associatorlevel;
	}
	public void setAssociatorlevel(String associatorlevel) {
		this.associatorlevel = associatorlevel;
	}
	public String getAssociatorexpirydate() {
		return associatorexpirydate;
	}
	public void setAssociatorexpirydate(String associatorexpirydate) {
		this.associatorexpirydate = associatorexpirydate;
	}
	
	
}
