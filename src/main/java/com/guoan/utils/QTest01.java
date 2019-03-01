package com.guoan.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;

public class QTest01 {

	public static void main(String[] args) throws Exception {
		
//		  Class.forName("com.cloudera.impala.jdbc41.Driver"); // 注册驱动
		  Class.forName("org.apache.hive.jdbc.HiveDriver"); // 注册驱动
		   Connection connection = DriverManager.getConnection("jdbc:hive2://10.10.40.1:21050/datacube_kudu"); // 获取连接
		   
		   String sql = "select name from datacube_kudu.d_eshop  where id = '00000000000000000000000000000037' ";
		   ResultSet query = connection.createStatement().executeQuery(sql);
		   
		   while(query.next()){
	   		System.out.println(query.getObject(1) );
	   		}
		
	}
}
