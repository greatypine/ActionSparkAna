package com.guoan.hive;

import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.guoan.pojo.StorePosition;
import com.guoan.utils.FileUtil;
import com.mongodb.spark.MongoSpark;

/**
  * Description:  mongo 数据库中的 store_position 表 导入hive中 
  * @author lyy  
  * @date 2018年5月28日
 */
public class StorePosition2Hive {
	
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
		 mongoTable = "store_position";
		
		 String tableName = "store_position";
		 
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
	    Dataset<StorePosition> explicitDS = MongoSpark.load(jsc).toDS(StorePosition.class).repartition(1);
		//注册为临时表
	    explicitDS.createOrReplaceTempView("store_pos");
		
		String sql =     " SELECT _id as id ,                                    "
						+"       status,                                     "
						+"       location[0] as longitude,                                          "
						+"       location[1] as latitude,                                       "
						+"       white                                   "
						+" FROM store_pos                                        ";
		
		JavaRDD<Row> javaRDD = spark.sql(sql).javaRDD();
		
		JavaRDD<StorePosition> resultRDD = javaRDD.mapPartitions(new FlatMapFunction<Iterator<Row>, StorePosition>() {
			private static final long serialVersionUID = -6589677182130607900L;

			@Override
			public Iterator<StorePosition> call(Iterator<Row> iterator) throws Exception {
				List<StorePosition> reList = new ArrayList<StorePosition>();
				//大循环
				while(iterator.hasNext()){
					Row row = iterator.next();
					StorePosition sp = new StorePosition();
					sp.setId(row.getAs("id") == null ? null : row.getAs("id")+"");
					sp.setLatitude(row.getAs("latitude") == null ? null : row.getAs("latitude")+"");
					sp.setLongitude(row.getAs("longitude") == null ? null : row.getAs("longitude")+"");
					sp.setStatus(row.getAs("status") == null ? null : row.getAs("status")+"");
					reList.add(sp);
				}
				
				return reList.iterator();
			}
		});
		
		 //写入hive
		 Dataset<Row> createDataFrameRDD = spark.createDataFrame(resultRDD, StorePosition.class);
		 try {
			createDataFrameRDD.createTempView("store_pos_2");
		} catch (AnalysisException e) {
			System.out.println("store_pos_2 临时表创建失败");
			e.printStackTrace();
		}
		
		 //删除hive表,如果存在
		 spark.sql("drop table if exists gabase."+tableName+" purge").count();
		 //spark临时表写入hive
		 spark.sql("create table gabase."+tableName+" as  select id ,latitude ,longitude ,status ,white  from store_pos_2" ).count();
		 jsc.close();
		 spark.close();
		 
	}
	
}
