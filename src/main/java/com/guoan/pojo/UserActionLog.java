package com.guoan.pojo;

import java.io.Serializable;

/**
  * Description:  user_action_log 
  * @author lyy  
  * @date 2018年5月4日
 */
public class UserActionLog implements Serializable {
	private static final long serialVersionUID = 1234470107907183131L;
	private String _id ;
	private Integer num ;
	private String booth ;
	private String cell ;
	private String exh ;
	private String jc ;
	private String jt ;
	private String phone ;
	private String time ;
	private String type ;
	private String mongo_time ;
	
	public String get_id() {
		return _id;
	}
	public void set_id(String _id) {
		this._id = _id;
	}
	public Integer getNum() {
		return num;
	}
	public void setNum(Integer num) {
		this.num = num;
	}
	public String getBooth() {
		return booth;
	}
	public void setBooth(String booth) {
		this.booth = booth;
	}
	public String getCell() {
		return cell;
	}
	public void setCell(String cell) {
		this.cell = cell;
	}
	public String getExh() {
		return exh;
	}
	public void setExh(String exh) {
		this.exh = exh;
	}
	public String getJc() {
		return jc;
	}
	public void setJc(String jc) {
		this.jc = jc;
	}
	public String getJt() {
		return jt;
	}
	public void setJt(String jt) {
		this.jt = jt;
	}
	public String getPhone() {
		return phone;
	}
	public void setPhone(String phone) {
		this.phone = phone;
	}
	public String getTime() {
		return time;
	}
	public void setTime(String time) {
		this.time = time;
	}
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	public String getMongo_time() {
		return mongo_time;
	}
	public void setMongo_time(String mongo_time) {
		this.mongo_time = mongo_time;
	}
	public String getCreate_time() {
		return create_time;
	}
	public void setCreate_time(String create_time) {
		this.create_time = create_time;
	}
	private String create_time ;
	
	
	
}
