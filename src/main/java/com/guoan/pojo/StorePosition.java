package com.guoan.pojo;

import java.io.Serializable;

/**
  * Description: 门店位置 
  * @author lyy  
  * @date 2019年2月12日
 */
public class StorePosition implements Serializable{

	private static final long serialVersionUID = 1L;

	private String _id ;
	private String id ;
	private String status ;
	private String location[];
	//经度
	private String longitude;
	//纬度
	private String latitude ;
	private String white ;
	
	public String getWhite() {
		return white;
	}
	public void setWhite(String white) {
		this.white = white;
	}
	public String get_id() {
		return _id;
	}
	public void set_id(String _id) {
		this._id = _id;
	}
	public String[] getLocation() {
		return location;
	}
	public void setLocation(String[] location) {
		this.location = location;
	}
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getStatus() {
		return status;
	}
	public void setStatus(String status) {
		this.status = status;
	}
	public String getLongitude() {
		return longitude;
	}
	public void setLongitude(String longitude) {
		this.longitude = longitude;
	}
	public String getLatitude() {
		return latitude;
	}
	public void setLatitude(String latitude) {
		this.latitude = latitude;
	}
	
	
}
