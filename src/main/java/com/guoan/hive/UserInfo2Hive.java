package com.guoan.hive;

import java.net.URLEncoder;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

import com.guoan.pojo.Register_customer_device_info;
import com.guoan.utils.FileUtil;
import com.mongodb.spark.MongoSpark;

/**
  * Description: (灰名单) mongo 数据库中的 userinfo 表 导入hive中 
  * @author lyy  
  * @date 2018年5月28日
 */
public class UserInfo2Hive {
	
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
		 mongoTable = "register_customer_device_info";
		
		 String tableName = "register_customer_device_info";
		 
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
	    Dataset<Register_customer_device_info> explicitDS = MongoSpark.load(jsc).toDS(Register_customer_device_info.class).repartition(1);
		//注册为临时表
	    explicitDS.createOrReplaceTempView("user_info");
		
		String sql =     " SELECT id,                                    "
						+"       deviceNum,                                     "
						+"       createTime,                                          "
						+"       customerId,                                       "
						+"       deviceos                                   "
						+" FROM user_info         where createTime > '2018-07-01'                               ";
		
		JavaRDD<Row> javaRDD = spark.sql(sql).javaRDD();
		
		JavaRDD<Register_customer_device_info> resultRDD = javaRDD.mapPartitions(new FlatMapFunction<Iterator<Row>, Register_customer_device_info>() {
			private static final long serialVersionUID = -6589677182130607900L;

			@Override
			public Iterator<Register_customer_device_info> call(Iterator<Row> iterator) throws Exception {
				List<Register_customer_device_info> reList = new ArrayList<Register_customer_device_info>();
				//大循环
				while(iterator.hasNext()){
					Row row = iterator.next();
					Register_customer_device_info ri = new Register_customer_device_info();
					ri.setId(row.getAs("id") == null ? null : row.getAs("id")+"");
					ri.setDeviceNum(row.getAs("deviceNum") == null ? null : row.getAs("deviceNum")+"");
					ri.setCreateTime(row.getAs("createTime") == null ? null : row.getAs("createTime")+"");
					ri.setCustomerId(row.getAs("customerId") == null ? null : row.getAs("customerId")+"");
					ri.setDeviceos(row.getAs("deviceos") == null ? null : row.getAs("deviceos")+"");
					reList.add(ri);
				}
				
				return reList.iterator();
			}
		});
		
		 //写入hive
		 Dataset<Row> createDataFrameRDD = spark.createDataFrame(resultRDD, Register_customer_device_info.class);
		 try {
			createDataFrameRDD.createTempView("user_info_2");
		} catch (AnalysisException e) {
			System.out.println("user_info_2 临时表创建失败");
			e.printStackTrace();
		}
		
		 //删除hive表,如果存在
		 spark.sql("drop table if exists gemini."+tableName+" purge").count();
		 //spark临时表写入hive
		 spark.sql("create table gemini."+tableName+" as  select * from user_info_2" ).count();
		 jsc.close();
		 spark.close();
		 
	}
	
	@Test
	public void test01(){
		
		Date date = new Date(1536910877000L);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		String format = sdf.format(date);
		System.out.println(format);
		
	}
	
}
