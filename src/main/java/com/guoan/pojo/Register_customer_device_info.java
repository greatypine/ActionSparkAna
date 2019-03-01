package com.guoan.pojo;

import java.io.Serializable;

/**
  * Description:   用户注册设备信息表
  * @author lyy  
  * @date 2018年12月25日
 */
public class Register_customer_device_info implements Serializable {

	private static final long serialVersionUID = 1L;
	
	private String id ;
	private String deviceNum;
	private String createTime;
	private String customerId;
	private String deviceos;
	
	
	
	public String getDeviceos() {
		return deviceos;
	}
	public void setDeviceos(String deviceos) {
		this.deviceos = deviceos;
	}
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getDeviceNum() {
		return deviceNum;
	}
	public void setDeviceNum(String deviceNum) {
		this.deviceNum = deviceNum;
	}
	public String getCreateTime() {
		return createTime;
	}
	public void setCreateTime(String createTime) {
		this.createTime = createTime;
	}
	public String getCustomerId() {
		return customerId;
	}
	public void setCustomerId(String customerId) {
		this.customerId = customerId;
	}
	
	
	
}
