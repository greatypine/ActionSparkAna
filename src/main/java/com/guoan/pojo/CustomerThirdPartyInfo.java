package com.guoan.pojo;

import java.io.Serializable;

/**
 * 
  * Description:   
  * @author lyy  
  * @date 2019年2月13日
 */
public class CustomerThirdPartyInfo implements Serializable{
	
	private static final long serialVersionUID = 1L;
	
	private String id;
	private String thirdPartyId;
	private String createTime;
	private String phone;
	private String customerId;
	private String source;
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getThirdPartyId() {
		return thirdPartyId;
	}
	public void setThirdPartyId(String thirdPartyId) {
		this.thirdPartyId = thirdPartyId;
	}
	public String getCreateTime() {
		return createTime;
	}
	public void setCreateTime(String createTime) {
		this.createTime = createTime;
	}
	public String getPhone() {
		return phone;
	}
	public void setPhone(String phone) {
		this.phone = phone;
	}
	public String getCustomerId() {
		return customerId;
	}
	public void setCustomerId(String customerId) {
		this.customerId = customerId;
	}
	public String getSource() {
		return source;
	}
	public void setSource(String source) {
		this.source = source;
	}
	
	
}
