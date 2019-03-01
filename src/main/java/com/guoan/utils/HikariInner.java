package com.guoan.utils;


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;


/**
 * 
  * Description:  Hikari连接池 
  * @author lyy  
  * @date 2018年7月18日
 */
public class HikariInner {
	//定义 Hikari 连接池配置对象
    private static  HikariConfig poolConfig = null;
    //定义 Hikari 连接池对象
    private static HikariDataSource dataSource = null;
    
    private static final String CONNECTION_URL = "jdbc:impala://124.93.28.145:17776/datacube;auth=noSasl";
    private static final String JDBC_DRIVER_NAME = "com.cloudera.impala.jdbc41.Driver";

//    private static final String CONNECTION_URL = CustomizedPropertyConfigurer.getCxtPropertisMap("CONNECTION_URL").toString();
//    private static final String JDBC_DRIVER_NAME = CustomizedPropertyConfigurer.getCxtPropertisMap("JDBC_DRIVER_NAME").toString();
//
    static{
        try {
        	 poolConfig = new HikariConfig();
        	 
        	 //基本配置
         	  poolConfig.setDriverClassName(JDBC_DRIVER_NAME);
         	  poolConfig.setJdbcUrl(CONNECTION_URL);
         	  //等待连接池分配连接的最大时长（毫秒），超过这个时长还没可用的连接则发生SQLException， 缺省:30秒 
         	  poolConfig.setConnectionTimeout(10000);
         	  //个连接idle状态的最大时长（毫秒），超时则被释放（retired），缺省:10分钟
         	  poolConfig.setIdleTimeout(60000);
         	  //一个连接的生命时长（毫秒），超时而且没被使用则被释放（retired），缺省:30分钟，建议设置比数据库超时时长少30秒，参考MySQL wait_timeout参数（show variables like '%timeout%';） 
         	  poolConfig.setMaxLifetime(1800000);
         	  //连接池中允许的最大连接数。缺省值：10；推荐的公式：((core_count * 2) + effective_spindle_count) 
         	  poolConfig.setMaximumPoolSize(30);
         	  poolConfig.setMinimumIdle(10);
        	 
          	  dataSource = new HikariDataSource(poolConfig);
          	
          	  System.out.println();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //获取与指定数据库的连接
    public static HikariDataSource getInstance(){
        return dataSource;
    }

    //从连接池返回一个连接
    public static Connection getConnection(){
        Connection conn = null;
        try {
            conn = dataSource.getConnection();
            
        } catch (Exception e) {
            e.printStackTrace();
        }
        return conn;
    }

    //释放资源
    public static void realeaseResource(ResultSet rs,PreparedStatement ps,Connection conn){
        if(null != rs){
            try {
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        if(null != ps){
            try {
                ps.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        try {
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    
    public static void main(String[] args) throws Exception {
	
    	Connection connection = getConnection();
//    	ResultSet result =connection.createStatement().executeQuery(" select name from datacube.d_eshop   where id = '00000000000000000000000000000036'");
    	ResultSet result =connection.createStatement().executeQuery("(select count(distinct tcb.id) as y,concat('x',tcb.sex )as x  from datacube.t_customer_base tcb group by x)union(select  count(distinct tcb.id) as y,concat('score',case when tcb.score<100 then'小于100' when tcb.score<1000 then '小于1000' when tcb.score<5000 then '小于5000' when tcb.score<10000 then '小于10000' WHEN tcb.score IS NULL THEN '等于0' else '大于等于10000' end )as scores  from datacube.t_customer_base tcb group by scores)union (select count(distinct tcb.id) as y,concat('channel',tcb.customer_source) as channel from datacube.t_customer_base tcb group by channel)union (select count(distinct tcb.id) as y,concat('system',tcb.device_os) as system from datacube.t_customer_base tcb group by system)union (select count(distinct tcb.id) as y,concat('level',tcb.associator_level) as level from datacube.t_customer_base tcb group by level)union (select count(distinct tcb.id) as y,concat('visit',case WHEN strleft(last_login,10)=to_date(subdate(now(),1)) THEN '昨日'WHEN  strleft(last_login,10)>to_date(subdate(now(),3)) and strleft(last_login,10)<to_date(subdate(now(),1))  THEN '最近3日'WHEN  strleft(last_login,10)>to_date(subdate(now(),7))  and strleft(last_login,10)<to_date(subdate(now(),3)) THEN '最近7日'WHEN  strleft(last_login,10)>to_date(subdate(now(),30)) and strleft(last_login,10)<to_date(subdate(now(),7)) THEN '最近30日'WHEN  strleft(last_login,10)>to_date(subdate(now(),90)) and strleft(last_login,10)<to_date(subdate(now(),30)) THEN '最近90日'ELSE '其他' END) as visit  from datacube.t_customer_base tcb group by visit)union (select count(distinct tcb.id) as y,concat('times',CASE when tcb.visit_num=1 THEN '1次'WHEN tcb.visit_num<3 THEN  '小于3次'WHEN tcb.visit_num<10 THEN '小于10次'WHEN tcb.visit_num<30 THEN '小于30次'when tcb.visit_num>=30 THEN '大于30次'END )AS times  from datacube.t_customer_base tcb group by times)");
        while(result.next()){
    		System.out.println(result.getObject(1)+""+result.getObject(1) );
    	}
    	
        result.close();
        connection.close();
    	
	}
    
    
}
