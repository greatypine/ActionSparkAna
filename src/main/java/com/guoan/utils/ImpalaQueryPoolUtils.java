package com.guoan.utils;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.mchange.v2.c3p0.ComboPooledDataSource;



/**
 * Created by gad on 2017/11/16.
 */
public  class ImpalaQueryPoolUtils  implements Serializable{

	private static final long serialVersionUID = -2870933121573193279L;
	
	
	private static String JDBC_DRIVER_NAME = "com.cloudera.impala.jdbc41.Driver";
//    private static String CONNECTION_URL = "jdbc:impala://192.168.1.111:21050/gemini";
    //北京
  private static String CONNECTION_URL = "jdbc:impala://10.10.40.1:21050/gemini";
    // 定义数据库的链接
    private static Connection connection;

    // 定义sql语句的执行对象
    private static PreparedStatement pstmt;

    // 定义查询返回的结果集合
    private static ResultSet resultSet;
    
    //定义连接池对象
    private static ComboPooledDataSource pool; 
    
    //加载连接池
    static{
      try{
    	  System.out.println("start=====initpool==========");
	 	  pool = new ComboPooledDataSource(); 
	 	  pool.setDriverClass(JDBC_DRIVER_NAME); 
	 	  pool.setJdbcUrl(CONNECTION_URL); 
	 	  pool.setMaxPoolSize(100);
	 	  pool.setMinPoolSize(20);
	   	  // 初始化连接池中的连接数，取值应在minPoolSize与maxPoolSize之间，默认为3
	   	  pool.setInitialPoolSize(50);
	   	  //最大空闲时间，30秒内未使用则连接被丢弃。若为0则永不丢弃。默认值: 0   
	   	  pool.setMaxIdleTime(1800);
	   	  // 当连接池连接耗尽时，客户端调用getConnection()后等待获取新连接的时间，超时后将抛出SQLException，如设为0则无限期等待。单位毫秒。默认: 0    
	   	  pool.setCheckoutTimeout(5000);
	   	  //当连接池中的连接耗尽的时候c3p0一次同时获取的连接数。默认值: 3    
	   	  pool.setAcquireIncrement(2); 
	   	  //定义在从数据库获取新连接失败后重复尝试的次数。默认值: 30 ；小于等于0表示无限次   
	   	  pool.setAcquireRetryAttempts(0);  
	   	  //重新尝试的时间间隔，3000毫秒   
	   	  pool.setAcquireRetryAttempts(3000);
	   	  //60秒检查空闲连接
	   	  pool.setIdleConnectionTestPeriod(60);
     	  
     	 System.out.println("end=====initpool==========");
     	   
      }catch(Exception e){
    	  System.out.println("c3p0连接池出现问题");
    	  e.printStackTrace();
      }
    }
    
    
    /**
     * 获取数据库连接
     *
     * @return 数据库连接
     */
    public static Connection getConnection() throws SQLException {
    	
    	if(pool == null){
    		try {
                Class.forName(JDBC_DRIVER_NAME); // 注册驱动
                System.out.println("非连接池注册驱动");
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
            connection = DriverManager.getConnection(CONNECTION_URL); // 获取连接
    	}else{
    		System.out.println("空闲连接数 === "+pool.getNumIdleConnections());
    		System.out.println("正在使用连接数 === "+pool.getNumBusyConnections());
    		connection = pool.getConnection();
    	}
        return connection;
    }

    /**
     * 释放资源
     */
    public static int releaseConn() {
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (SQLException e) {
                e.printStackTrace();
                return -203;
            }
        }
        if (pstmt != null) {
            try {
                pstmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
                return -204;
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
                return -205;
            }
        }
        return 200;
    }


    /**
     * 获取条件查询指定偏移数据
     *
     * @param sql sql语句
     * @param offset 偏移量
     * @param limit 获取条数
     * @return rr ReturnResult
     * @throws SQLException
     */
    public ReturnResult findResultPage(String sql, int offset, int limit) {

        ReturnResult rr = new ReturnResult();
        //rr.setMsg("分页查询成功");
        if(limit<1||offset<0){
            rr.setReturnCode(-100);
            rr.setMsg("error-参数错误：页面size(limit)必须>=1且offset必须>=0");
            return rr;
        }

        //获取查询数据其中的指定偏移数据
        String newSql = sql + " LIMIT " + limit + " OFFSET " + offset;
        try {
            List<Map<String, Object>> list = getList(newSql);
            Long total = getTotal(sql);
            if(list==null||total==null){
                rr.setReturnCode(-101);
                rr.setMsg("error-获取通往impala连接失败：请检查impala集群是否运行正常，网络是否通畅！");
                return rr;
            }
            rr.setRows(list);
            rr.setTotal(total);
        } catch (SQLException e) {
            e.printStackTrace();
            rr.setReturnCode(-102);
            rr.setMsg("error-sql执行异常：SQLException:"+e.getMessage().substring(0,100));
        } finally {
            int flag = releaseConn();
            if(flag<0){
                rr.setReturnCode(-103);
                rr.setMsg("error-资源关闭异常：ResultSet／PreparedStatement／Connection未能正确关闭！");
            }
        }
        int flag = releaseConn();
        if(flag<0){
           System.out.println("error-资源关闭异常：ResultSet／PreparedStatement／Connection未能正确关闭！");
        }
        return rr;
    }

    /**
     * 获取条件查询数据
     *
     * @param sql sql语句
     * @return rr ReturnResult
     * @throws SQLException
     */
    public static ReturnResult findResult(String sql) {

        ReturnResult rr = new ReturnResult();
        try {
        	List<Map<String, Object>> list = getList(sql);
            if(list==null){
                rr.setReturnCode(-101);
                rr.setMsg("error-获取通往impala连接失败：请检查impala集群是否运行正常，网络是否通畅！");
                return rr;
            }
            rr.setRows(list);
            rr.setTotal(list.size());
        } catch (SQLException e) {
            e.printStackTrace();
            rr.setReturnCode(-102);
            rr.setMsg("error-sql执行异常：SQLException:"+e.getMessage().substring(0,100));
        } finally {
            int flag = releaseConn();
            if(flag<0){
                rr.setReturnCode(-103);
                rr.setMsg("error-资源关闭异常：ResultSet／PreparedStatement／Connection未能正确关闭！");
            }
        }
        return rr;
    }

    public static List<Map<String, Object>> getList(String sql) throws SQLException {
    	
    	System.out.println(sql);
    	
        try {
            connection = getConnection();
        } catch (SQLException e) {
            //获取连接失败
            e.printStackTrace();
            return null;
        }
        List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
        //获取查询数据总数
        pstmt = connection.prepareStatement(sql);
        resultSet = pstmt.executeQuery();
        ResultSetMetaData metaData = resultSet.getMetaData();
        int cols_len = metaData.getColumnCount();
        while (resultSet.next()) {
            Map<String, Object> map = new HashMap<String, Object>();
            for (int i = 0; i < cols_len; i++) {
                String cols_name = metaData.getColumnName(i + 1);
                Object cols_value = resultSet.getObject(cols_name);
                if (cols_value == null) {
                    cols_value = "";
                }
                map.put(cols_name, cols_value);
            }
            list.add(map);
        }
        
        int flag = releaseConn();
        if(flag<0){
           System.out.println("error-资源关闭异常：ResultSet／PreparedStatement／Connection未能正确关闭！");
        }
        
        return list;
    }
    

    public static Long getTotal(String sql) throws SQLException {
        //获取查询数据总数
        try {
            connection = getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
        pstmt = connection.prepareStatement(sql);
        resultSet = pstmt.executeQuery();
        long total = 0;
        while (resultSet.next()) {
            total++;
        }
        
        int flag = releaseConn();
        if(flag<0){
           System.out.println("error-资源关闭异常：ResultSet／PreparedStatement／Connection未能正确关闭！");
        }
        return total;
    }
    
    /**
     * @param list
     * @return Map<String,Boolean> key:执行的sql语句    value:true 说明语句有结果集，false 语句没有结果集，如更改
     */
    public static Map<String,Boolean> executeList(List<String> list){
    	Map<String,Boolean> map = new HashMap<String, Boolean>();
    	
    	   try {
               connection = getConnection();
               for (String sql : list) {
            	   pstmt = connection.prepareStatement(sql);
                   boolean execute = pstmt.execute();
                   System.out.println(sql+"===="+execute);
                   map.put(sql, execute);
               }
               
           } catch (SQLException e) {
               e.printStackTrace();
               return null;
           }finally{
        	   int flag = releaseConn();
               if(flag<0){
                  System.out.println("error-资源关闭异常：ResultSet／PreparedStatement／Connection未能正确关闭！");
               }
           }
    	return map;
    }

    

}
