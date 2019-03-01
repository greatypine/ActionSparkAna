package com.guoan.pojo;

import java.io.Serializable;
import java.util.Map;

/**
  * Description:  用户信息类 
  * @author lyy  
  * @date 2018年5月23日
 */
public class CustomerInfoRecord implements Serializable{

	private static final long serialVersionUID = 1452747783973552086L;
	
	private String _id; //id
	private String createdAt; 
	private String[] userDefine; //用户定义,已婚,未婚等
	private String createTime; //创建时间
	private String phone; //电话
	private String cityCode; //城市编号
	private String inviteCode; //城市编号
	private String idCard; //身份证号
	private String customerId; //用户id
	private String name; //姓名
	private String birthday;//生日
	private Map<String,String> idCardInfo; //身份证信息
	private String[] economyStrength; //
	private String[] hobby; //爱好
	private String associatorLevel;
	private String associatorExpiryDate;
	
	
	
	
	public String getInviteCode() {
		return inviteCode;
	}
	public void setInviteCode(String inviteCode) {
		this.inviteCode = inviteCode;
	}
	public String getBirthday() {
		return birthday;
	}
	public void setBirthday(String birthday) {
		this.birthday = birthday;
	}
	public String getAssociatorLevel() {
		return associatorLevel;
	}
	public void setAssociatorLevel(String associatorLevel) {
		this.associatorLevel = associatorLevel;
	}
	public String getAssociatorExpiryDate() {
		return associatorExpiryDate;
	}
	public void setAssociatorExpiryDate(String associatorExpiryDate) {
		this.associatorExpiryDate = associatorExpiryDate;
	}
	public String get_id() {
		return _id;
	}
	public void set_id(String _id) {
		this._id = _id;
	}
	public String getCreatedAt() {
		return createdAt;
	}
	public void setCreatedAt(String createdAt) {
		this.createdAt = createdAt;
	}
	public String[] getUserDefine() {
		return userDefine;
	}
	public void setUserDefine(String[] userDefine) {
		this.userDefine = userDefine;
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
	public String getCityCode() {
		return cityCode;
	}
	public void setCityCode(String cityCode) {
		this.cityCode = cityCode;
	}
	public String getIdCard() {
		return idCard;
	}
	public void setIdCard(String idCard) {
		this.idCard = idCard;
	}
	public String getCustomerId() {
		return customerId;
	}
	public void setCustomerId(String customerId) {
		this.customerId = customerId;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	
	public Map<String, String> getIdCardInfo() {
		return idCardInfo;
	}
	public void setIdCardInfo(Map<String, String> idCardInfo) {
		this.idCardInfo = idCardInfo;
	}
	
	public String[] getEconomyStrength() {
		return economyStrength;
	}
	public void setEconomyStrength(String[] economyStrength) {
		this.economyStrength = economyStrength;
	}
	public String[] getHobby() {
		return hobby;
	}
	public void setHobby(String[] hobby) {
		this.hobby = hobby;
	}

	
	
}
