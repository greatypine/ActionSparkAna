package com.guoan.pojo;

import java.io.Serializable;

/**
  * Description:活动封装类,用于写入hive表中
  * @author lyy  
  * @date 2018年5月21日
 */
public class ActivityResult implements Serializable{

	private static final long serialVersionUID = 8257295893692435219L;
	//唯一标识id
	private String id;
	//优惠券id
	private String type_id ;
	//团购id
	private String groupon_instance_id;
	//sku活动id
	private String sku_rule_id ;
	//省id
	private String province_code;
	//市id
	private String city_code;
	//区id
	private String ad_code;
	//事业群id
	private String bussiness_group_id;
	//频道id
	private String channel_id;
	//拉新人数
	private Integer newUser ;
	//回流人数
	private Integer returnUser ;
	//总销量
	private Long saleNum ;
	//时间 yyyy-MM-dd形式
	private String simple_date;
	
	//时间类型,如果是分析时间段就是输入时间段,如果是分析昨天就是  'yesterday'
	private String date_type;
	
	//活动标识
	private String activity_mark;
	
	public String getDate_type() {
		return date_type;
	}
	public void setDate_type(String date_type) {
		this.date_type = date_type;
	}
	public String getActivity_mark() {
		return activity_mark;
	}
	public void setActivity_mark(String activity_mark) {
		this.activity_mark = activity_mark;
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
	public Long getSaleNum() {
		return saleNum;
	}
	public void setSaleNum(Long saleNum) {
		this.saleNum = saleNum;
	}
	//总销售额
	private Double saleMoneg;
	public String getType_id() {
		return type_id;
	}
	public void setType_id(String type_id) {
		this.type_id = type_id;
	}
	public String getGroupon_instance_id() {
		return groupon_instance_id;
	}
	public void setGroupon_instance_id(String groupon_instance_id) {
		this.groupon_instance_id = groupon_instance_id;
	}
	public String getSku_rule_id() {
		return sku_rule_id;
	}
	public void setSku_rule_id(String sku_rule_id) {
		this.sku_rule_id = sku_rule_id;
	}
	public String getProvince_code() {
		return province_code;
	}
	public void setProvince_code(String province_code) {
		this.province_code = province_code;
	}
	public String getCity_code() {
		return city_code;
	}
	public void setCity_code(String city_code) {
		this.city_code = city_code;
	}
	public String getAd_code() {
		return ad_code;
	}
	public void setAd_code(String ad_code) {
		this.ad_code = ad_code;
	}
	public String getBussiness_group_id() {
		return bussiness_group_id;
	}
	public void setBussiness_group_id(String bussiness_group_id) {
		this.bussiness_group_id = bussiness_group_id;
	}
	public String getChannel_id() {
		return channel_id;
	}
	public void setChannel_id(String channel_id) {
		this.channel_id = channel_id;
	}
	public Integer getNewUser() {
		return newUser;
	}
	public void setNewUser(Integer newUser) {
		this.newUser = newUser;
	}
	public Integer getReturnUser() {
		return returnUser;
	}
	public void setReturnUser(Integer returnUser) {
		this.returnUser = returnUser;
	}
	public Double getSaleMoneg() {
		return saleMoneg;
	}
	public void setSaleMoneg(Double saleMoneg) {
		this.saleMoneg = saleMoneg;
	}
	
}
