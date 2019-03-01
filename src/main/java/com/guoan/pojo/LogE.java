package com.guoan.pojo;

import java.io.Serializable;

/**
  * Description:   E店用户行为日志(并集)
  * @author lyy  
  * @date 2018年6月4日
 */
public class LogE implements Serializable ,Cloneable {

	private static final long serialVersionUID = -3114229494882227268L;
	//唯一标识
	private String id ;
	//微信号,md5加密的
	private String opean_id;
	//requestTimestamp
	//时间戳,共有
	private String create_date;
	//ip
	private String ip;
	//门店id
	private String store_id;
	//用户id
	private String customer_id;
	//设备类型
	private String equipment_type;
	//网卡地址
	private String mac_address;
	//json格式日志==========
	//类型id
	private String type_id;
	//类型,对type_id的说明
	private String type;
	//行为类型
	private String task;
	//(shoppe_id , eshop_id )
	//e店id; eshopSchedule , eshopinfo,eshopprolist 行为
	private String eshop_id;
	//下单行为的订单id
	private String order_id;
	//电话号码,方便找回customer_id
	private String phone;
	//日志类型 (mongo_log , all_log)
	//all_log , 上面的字段
	//mongo_log, userActSta行为发送给mongo的数据
	private String log_type;
	//===========mongo_log=============
	private String booth;
	private String cell;
	private String exh;
	private String jc;
	private String jt;
	//标识其为一个系列(data)
	private String single_tag;
	//商品id
	private String product_id;
	
	

	public String getProduct_id() {
		return product_id;
	}

	public void setProduct_id(String product_id) {
		this.product_id = product_id;
	}

	public String getSingle_tag() {
		return single_tag;
	}

	public void setSingle_tag(String single_tag) {
		this.single_tag = single_tag;
	}

	public String getLog_type() {
		return log_type;
	}

	public void setLog_type(String log_type) {
		this.log_type = log_type;
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


	public String getOrder_id() {
		return order_id;
	}

	public void setOrder_id(String order_id) {
		this.order_id = order_id;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}
	
	public String getType_id() {
		return type_id;
	}

	public void setType_id(String type_id) {
		this.type_id = type_id;
	}
	
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getOpean_id() {
		return opean_id;
	}

	public void setOpean_id(String opean_id) {
		this.opean_id = opean_id;
	}

	public String getCreate_date() {
		return create_date;
	}

	public void setCreate_date(String create_date) {
		this.create_date = create_date;
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public String getStore_id() {
		return store_id;
	}

	public void setStore_id(String store_id) {
		this.store_id = store_id;
	}

	public String getCustomer_id() {
		return customer_id;
	}

	public void setCustomer_id(String customer_id) {
		this.customer_id = customer_id;
	}

	public String getEquipment_type() {
		return equipment_type;
	}

	public void setEquipment_type(String equipment_type) {
		this.equipment_type = equipment_type;
	}

	public String getMac_address() {
		return mac_address;
	}

	public void setMac_address(String mac_address) {
		this.mac_address = mac_address;
	}

	public String getTask() {
		return task;
	}

	public void setTask(String task) {
		this.task = task;
	}

	public String getEshop_id() {
		return eshop_id;
	}

	public void setEshop_id(String eshop_id) {
		this.eshop_id = eshop_id;
	}
	
	 @Override  
    public Object clone() throws CloneNotSupportedException  
    {  
        return super.clone();  
    }

	@Override
	public String toString() {
		return "LogE [id=" + id + ", opean_id=" + opean_id + ", create_date=" + create_date + ", ip=" + ip
				+ ", store_id=" + store_id + ", customer_id=" + customer_id + ", equipment_type=" + equipment_type
				+ ", mac_address=" + mac_address + ", type_id=" + type_id + ", type=" + type + ", task=" + task
				+ ", eshop_id=" + eshop_id + ", order_id=" + order_id + ", phone=" + phone + ", log_type=" + log_type
				+ ", booth=" + booth + ", cell=" + cell + ", exh=" + exh + ", jc=" + jc + ", jt=" + jt + "]";
	}
	
	 
}
