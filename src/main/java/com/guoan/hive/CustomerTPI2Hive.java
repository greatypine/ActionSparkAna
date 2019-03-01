package com.guoan.hive;

import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.guoan.pojo.CustomerThirdPartyInfo;
import com.guoan.utils.FileUtil;
import com.mongodb.spark.MongoSpark;

/**
  * Description: 
  * @author lyy  
  * @date 2018年5月28日
 */
public class CustomerTPI2Hive {
	
	private static String mongoIp = null ;
	private static String mongoPassword = null ;
	private static String mongoPort = null ;
	private static String mongoDatabase = null ;
	private static String mongoTable = null ;
	private static String sparkLogLevel = null;
	private static final String configPath = "conf/config.properties";

	public static void main(String[] args)throws Exception {
		
		System.setProperty("user.name", "hdfs");
		
		 mongoIp = FileUtil.getProperties("mongoIp",configPath);
		 mongoPassword = FileUtil.getProperties("mongoPassword",configPath);
		 mongoPort = FileUtil.getProperties("mongoPort",configPath);
		 mongoDatabase = FileUtil.getProperties("mongoDatabase",configPath);
		 sparkLogLevel = FileUtil.getProperties("sparkLogLevel",configPath);
		 mongoTable = "customer_third_party_info";
		
		 String tableName = "customer_third_party_info";
		 
		//密码中有特殊字符,需要url编码
		String url = "mongodb://"+mongoDatabase+":"+URLEncoder.encode(mongoPassword,"UTF-8")+
				"@"+mongoIp+":"+mongoPort+"/"+mongoDatabase+"."+mongoTable;
		System.out.println(url);
		SparkSession spark = SparkSession.builder()
                .appName("CustomerInfoDemo1")
                .enableHiveSupport()
                .master("local")
                .config("spark.mongodb.input.uri", url)
                .config("spark.mongodb.output.uri", url)
                .getOrCreate();
		 // 创建jsc 对象
	    JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
	    jsc.setLogLevel(sparkLogLevel);
	    // Load data with explicit schema
	    Dataset<CustomerThirdPartyInfo> explicitDS = MongoSpark.load(jsc).toDS(CustomerThirdPartyInfo.class).repartition(1);
		//注册为临时表
	    explicitDS.createOrReplaceTempView("customer_info");
		
		String sql =     " SELECT thirdPartyId,                                    "
						+"       createTime,                                     "
						+"       phone,                                          "
						+"       customerId,                                       "
						+"       source                                   "
						+" FROM customer_info                                       ";
		
		JavaRDD<Row> javaRDD = spark.sql(sql).javaRDD();
		
		JavaRDD<CustomerThirdPartyInfo> resultRDD = javaRDD.mapPartitions(new FlatMapFunction<Iterator<Row>, CustomerThirdPartyInfo>() {
			private static final long serialVersionUID = -6589677182130607900L;

			@Override
			public Iterator<CustomerThirdPartyInfo> call(Iterator<Row> iterator) throws Exception {
				List<CustomerThirdPartyInfo> reList = new ArrayList<CustomerThirdPartyInfo>();
				//大循环
				while(iterator.hasNext()){
					Row row = iterator.next();
					CustomerThirdPartyInfo ri = new CustomerThirdPartyInfo();
					
					ri.setId(UUID.randomUUID().toString().replace("-", ""));
					ri.setCreateTime(row.getAs("createTime") == null ? null : row.getAs("createTime")+"");
					ri.setCustomerId(row.getAs("customerId") == null ? null : row.getAs("customerId")+"");
					ri.setPhone(row.getAs("phone") == null ? null : row.getAs("phone")+"");
					ri.setSource(row.getAs("source") == null ? null : row.getAs("source")+"");
					ri.setThirdPartyId(row.getAs("thirdPartyId") == null ? null : row.getAs("thirdPartyId")+"");
					
					reList.add(ri);
				}
				
				return reList.iterator();
			}
		});
		
		 //写入hive
		 Dataset<Row> createDataFrameRDD = spark.createDataFrame(resultRDD, CustomerThirdPartyInfo.class);
		 try {
			createDataFrameRDD.createTempView("customer_info_2");
		} catch (AnalysisException e) {
			System.out.println("customer_info_2 临时表创建失败");
			e.printStackTrace();
		}
		
		 //删除hive表,如果存在
		 spark.sql("drop table if exists daqweb."+tableName+" purge").count();
		 //spark临时表写入hive
		 spark.sql("create table daqweb."+tableName+" as  select * from customer_info_2" ).count();
		 jsc.close();
		 spark.close();
		 
	}
	
	
}
