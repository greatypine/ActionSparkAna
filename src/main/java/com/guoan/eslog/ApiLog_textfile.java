package com.guoan.eslog;


import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

import com.alibaba.fastjson.JSONException;
import com.guoan.eslog.savefile.Api_log_saveTextfile;
import com.guoan.mongo.ReadMongo2File;
import com.guoan.pojo.ApiLog;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

/**
  * Description:   直接获取mongo数据
  * @author lyy  
  * @date 2018年6月13日
 */
public class ApiLog_textfile {
	
	private static final String tableName = "log_api";
	//分区数量
	private static final int partitionNum = 3;
	private static final String savePath = "hdfs://node401:8020/user/log/api/";
	
	public static void main(String[] args)  throws Exception{
		System.setProperty("user.name", "hdfs");
		
		//重建表还是insert表
		boolean isCreate = true;
		//输入两个参数,create  或者  insert 默认create
		if(args.length >0){
			String param = args[0];
			if(param!= null && !"".equals(param)){
				if("insert".equals(param.toLowerCase())){
					isCreate = false;
				}
			}
		}
		SimpleDateFormat simpleSdf = new SimpleDateFormat("yyyy-MM-dd");
		
		String yesterday = simpleSdf.format(  new Date(new Date().getTime()-24*60*60*1000));
		//获取数据
		Api_log_saveTextfile.saveApiFile2Hdfs(yesterday);
		
		SparkConf conf = new SparkConf();
        conf.set("spark.broadcast.compress", "true");// 设置广播压缩
        conf.set("spark.rdd.compress", "true");      // 设置RDD压缩
        conf.set("spark.io.compression.codec", "org.apache.spark.io.LZFCompressionCodec");
        conf.set("spark.reducer.maxSizeInFlight", "1024m");
        conf.set("spark.reducer.maxMblnFlight", "1024m");
        
		String path = savePath+yesterday.replace("-", "")+".log";
		 
		//=======spark======
		 //读取测试hive数据
		 SparkSession spark = SparkSession
			      .builder()
			      .config(conf)
			      .enableHiveSupport()
			      .appName("Spark Action_es_api")
			      .getOrCreate();
		 
		 JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
		 jsc.setLogLevel("INFO");
		
	    System.out.println("=========================");
		 
	    //读数据
	    JavaRDD<String> textFile = jsc.textFile(path , partitionNum);
	    /*
	     * 长度小于20 的, 为空的,不含requestUri的都过滤掉
	     */
	    JavaRDD<String> filterRDD = textFile.filter(new Function<String, Boolean>() {
			private static final long serialVersionUID = -6246456729630337449L;
			@Override
			public Boolean call(String line) throws Exception {
				
				if(line.length() <20){
					return false;
				}
				try{
					JSONObject fromObject = JSONObject.fromObject(line);
					if(fromObject.get("requestUri") == null){
						return false;
					}
				}catch(Exception e){
					System.out.println(line);
					return false;
				}
				
				return true;
			}
		});
	    
	    /*
	     * 生成对象
	     */
	    JavaRDD<ApiLog> resultRDD = filterRDD.mapPartitions(new FlatMapFunction<Iterator<String>, ApiLog>() {
			private static final long serialVersionUID = -2563878803032350641L;

			@Override
			public Iterator<ApiLog> call(Iterator<String> iterator) throws Exception {
				
				List<ApiLog> returnList = new ArrayList<ApiLog>();
				
				//大循环
				while(iterator.hasNext()){
					String line = iterator.next();
					//JSON转换
					JSONObject fromObject = JSONObject.fromObject(line);
					
					//解析json串 ,封装成对象
					try{
						List<ApiLog> apiLog = getApiLog(fromObject);
						for(ApiLog al  : apiLog){
							returnList.add(al);
						}
					}catch(Exception e){
						//可能出现转换错误
//						System.out.println("ERRRO : "+line);
					}
				}
				return returnList.iterator();
			}
		});
	    
	    
	    //写入hive ===== log_api
		 Dataset<Row> createDataFrameRDD = spark.createDataFrame(resultRDD, ApiLog.class);
		 try {
			createDataFrameRDD.createTempView("log_tmp_api_log");
		} catch (AnalysisException e) {
			System.out.println("log_tmp_api_log 临时表创建失败");
			e.printStackTrace();
		}
		 
		 if(isCreate){
			//有就删除
			 spark.sql("drop table if exists gemini."+tableName+" purge ").count();
			 spark.sql("create   table gemini."+tableName+" as   select * from log_tmp_api_log ").count();
		 }else{
			 //写入hive
			 spark.sql("insert into  table gemini."+tableName+"  select * from log_tmp_api_log").count();
		 }
	    
	    jsc.close();
	    spark.close();
	}
	
	/**
	  * Title: getApiLog
	  * Description: 封装要存入hive的对象
	  * @param fromObject
	  * @return
	  * @throws Exception
	 */
	public static List<ApiLog> getApiLog(JSONObject fromObject) throws Exception{
		List<ApiLog> list = new ArrayList<ApiLog>();
		
		ApiLog ag1 = new ApiLog(); 
		
	  //  private String id ;
		ag1.setId(fromObject.get("_id") == null ? null : fromObject.get("_id").toString());
      //private String requestUri;
		ag1.setRequestUri(fromObject.get("requestUri") == null ? null : fromObject.get("requestUri").toString());
	//		private String storeId; 
		ag1.setStore_id(fromObject.get("storeId") == null ? null : fromObject.get("storeId").toString());
	//		private String createTime; 
		ag1.setCreateTime(fromObject.get("createTime") == null ? null : fromObject.get("createTime").toString());
	//		private String createDate;
		ag1.setCreateDate(fromObject.get("createDate") == null ? null : fromObject.get("createDate").toString());
	//		private String token;
		ag1.setToken(fromObject.get("token") == null ? null : fromObject.get("token").toString());
	//		private String appTypePlatform;
		ag1.setMdop(fromObject.get("mdop") == null ? null : fromObject.get("mdop").toString());
		
		ag1.setAdtag(fromObject.get("adtag") == null ? null : fromObject.get("adtag").toString());
		
		ag1.setAppTypePlatform(fromObject.get("appTypePlatform") == null ? null : fromObject.get("appTypePlatform").toString());
		
		//json格式的字符串
		//解析requestData
		JSONObject requestData = fromObject.getJSONObject("requestData");
		if(!requestData.isNullObject()){
			//private String orderId;
			ag1.setOrder_id(requestData.get("orderId") == null? null :requestData.get("orderId").toString() );
			//payPlatform 支付方式
			ag1.setPayPlatform(requestData.get("payPlatform") == null? null :requestData.get("payPlatform").toString());
		}
		JSONObject responseData = fromObject.getJSONObject("responseData");
		if(!responseData.isNullObject()){
			//		private String message;
			ag1.setMessage(responseData.get("message") == null ? null :responseData.get("message").toString() );
		}
		
		//两个不能同时有,所以分别添加就行
		
		if(!requestData.isNullObject()){
			//private String groupIds;
			Object groupIdsObj = requestData.get("groupIds");
			if(groupIdsObj != null){
				JSONArray groupIdsArray = requestData.getJSONArray("groupIds");
				for (Object groupId : groupIdsArray) {
					ApiLog clone = (ApiLog) ag1.clone();
					clone.setId(UUID.randomUUID().toString().replace("-", ""));
					clone.setGroup_id(groupId.toString());
					list.add(clone);
				}
				return list;
			}
		}
		//解析responseData
		if(!responseData.isNullObject()){
			//private String orderGroupIds;
			if(responseData.get("data") != null ){
				
				try{
					JSONObject dataObject = responseData.getJSONObject("data");
					if(!dataObject.isNullObject()){
						Object orderGroupIdsObj = dataObject.get("orderGroupIds");
						if(orderGroupIdsObj !=null){
							JSONArray orderGroupIdsArray = dataObject.getJSONArray("orderGroupIds");
							//拆开
							for (Object orderGroupId : orderGroupIdsArray) {
								ApiLog clone = (ApiLog) ag1.clone();
								clone.setId(UUID.randomUUID().toString().replace("-", ""));
								clone.setGroup_id(orderGroupId.toString());
								list.add(clone);
							}
							return list;
						}
					}
				}catch(JSONException e){
					}
			}
		}
		list.add(ag1);
		return list;
	}
	
	
	@Test
	public void test01() throws Exception{
		
		String[] str = new String[]{"2018-06-14","2018-06-15"};
		
		for (String string : str) {
			 ReadMongo2File.rm2f("C:\\Users\\Administrator\\Desktop\\api.log"  , "createDate", string);
		}
	}
	
	
}
