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
import org.junit.Test;

import com.guoan.pojo.CustomerInfoRecord;
import com.guoan.pojo.CustomerInfoRecord_save;
import com.guoan.utils.FileUtil;
import com.mongodb.spark.MongoSpark;

/**
  * Description:  mongo 数据库中的 userinfo 表 导入hive中 
  * @author lyy  
  * @date 2018年5月28日
 */
public class CustomerInfoMongo2Hive {
	
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
		 mongoTable = "customer_info_record";
		
		 String tableName = "customer_info_record";
		 
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
	    Dataset<CustomerInfoRecord> explicitDS = MongoSpark.load(jsc).toDS(CustomerInfoRecord.class).repartition(1);
		//注册为临时表
	    explicitDS.createOrReplaceTempView("user_info");
		
		String sql =     " SELECT customerid,                                    "
						+"       createtime,                                     "
						+"       phone,                                          "
						+"       citycode,                                       "
						+"       inviteCode,                                       "
						+"       idcard,                                         "
						+"       name,                                           "
						+"       (CASE                                           "
						+"          WHEN ISNULL( idCardInfo['birthday'] )=FALSE  "
						+"          THEN idCardInfo['birthday']                  "
						+"          ELSE birthday                                "
						+"        END) AS birthday,                              "
						+"       idCardInfo['address'] AS address,               "
						+"       idCardInfo['sex'] AS sex,                       "
						+"       associatorLevel,                                "
						+"       associatorExpiryDate                            "
						+" FROM user_info                                        ";
		
		JavaRDD<Row> javaRDD = spark.sql(sql).javaRDD();
		
		JavaRDD<CustomerInfoRecord_save> resultRDD = javaRDD.mapPartitions(new FlatMapFunction<Iterator<Row>, CustomerInfoRecord_save>() {
			private static final long serialVersionUID = -6589677182130607900L;

			@Override
			public Iterator<CustomerInfoRecord_save> call(Iterator<Row> iterator) throws Exception {
				
				List<CustomerInfoRecord_save> returnList= new ArrayList<CustomerInfoRecord_save>();
				
				while(iterator.hasNext()){
					Row row = iterator.next();
					CustomerInfoRecord_save cir = new CustomerInfoRecord_save();
					cir.setId(UUID.randomUUID().toString().replace("-", ""));
					cir.setCustomerid(row.getAs("customerid") == null ? null : row.getAs("customerid")+"");
					cir.setCreatetime(row.getAs("createtime") == null ? null : row.getAs("createtime")+"");
					cir.setInviteCode(row.getAs("inviteCode") == null ? null : row.getAs("inviteCode")+"");
					cir.setPhone(row.getAs("phone") == null ? null : row.getAs("phone")+"");
					cir.setCitycode(row.getAs("citycode") == null ? null : row.getAs("citycode")+"");
					cir.setName(row.getAs("name") == null ? null : row.getAs("name")+"");
					cir.setAddress(row.getAs("address") == null ? null : row.getAs("address")+"");
					cir.setSex(row.getAs("sex") == null ? null : row.getAs("sex")+"");
					cir.setAssociatorlevel(row.getAs("associatorLevel") == null ? null : row.getAs("associatorLevel")+"");
					cir.setAssociatorexpirydate(row.getAs("associatorExpiryDate") == null ? null : row.getAs("associatorExpiryDate")+"");
					
					//idcard
					String idcard = row.getAs("idcard") == null ? null : row.getAs("idcard").toString().trim();
					//birthday
					String birthday = row.getAs("birthday") == null ? null : row.getAs("birthday")+"";
					
					if(birthday  == null && idcard != null && idcard.length() >10){
						StringBuffer b_one = new StringBuffer(idcard.substring(6, 14));
						b_one.insert(4, "-");
						b_one.insert(7, "-");
						birthday = b_one.toString();
					}
					cir.setIdcard(idcard);
					cir.setBirthday(birthday);
					returnList.add(cir);
				}
				
				return returnList.iterator();
			}
		});
		
		 //写入hive
		 Dataset<Row> createDataFrameRDD = spark.createDataFrame(resultRDD, CustomerInfoRecord_save.class);
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
	}
	
}
