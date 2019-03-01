package com.guoan.eslog;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.guoan.pojo.LogEM;

import scala.Tuple2;

/**
  * Description:   mongo类型的数据补到eshop大表中
  * @author lyy  
  * @date 2018年6月8日
 */
public class Eshop_log_textfile_two {
	
	private static final String getTableName = "log_eshop_one";
	//最终存储表
	private static final String tableName = "log_eshop_two";
	//分区数量
	private static final int partitionNum = 4; 
	
	public static void main(String[] args) {
		
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
		
		
		 //读取测试hive数据
		 SparkSession spark = SparkSession
			      .builder()
			      .appName("Spark Action_es_eshop_two")
			      .enableHiveSupport()
			      .getOrCreate();
		 
		 JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
		 jsc.setLogLevel("INFO");
		 //五种行为list
		 List<String> fiveActionList = Arrays.asList("websdk","H5","static","TOUTIAO","government");
		 final Broadcast<List<String>> broadcastFiveActionList = jsc.broadcast(fiveActionList);
		 
		 SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		 final Broadcast<SimpleDateFormat> broadcastSdf = jsc.broadcast(sdf);
		 
//		 //获取表数据
//		 String getTableName = logTableName+"_"+yesterday;
		 
		 String getLogTableSql = "select  "
		 		+ "id,opean_id, create_date, ip, store_id, customer_id, equipment_type, mac_address, type_id, type, task, eshop_id, order_id, phone ,product_id "
		 		+ " from gemini."+getTableName+" where log_type = 'all_log'  "
		 		+ " and create_date is not null ";
		 
		 String getMongoTableSql = "select "
		 		+ " id, create_date, customer_id, type_id, type, task, phone, exh, jc, jt , product_id "
		 		+ " from gemini."+getTableName+" where log_type = 'mongo_log' "
		 		+ " and create_date is not null ";
		 
		 Dataset<Row> logDataSet = spark.sql(getLogTableSql).repartition(partitionNum);
		 Dataset<Row> mongoDataSet = spark.sql(getMongoTableSql).repartition(partitionNum);
		 
		 //主线1 ,过滤留下 留下五种行为
		 JavaRDD<Row> RDD1filter = mongoDataSet.javaRDD().filter(new Function<Row, Boolean>() {
			private static final long serialVersionUID = 2065318289244867290L;
			@Override
			public Boolean call(Row row) throws Exception {
				
				List<String> fList = broadcastFiveActionList.value();
				//如果 jt 为空或者 不在五种行为之内,就过滤掉
				if(row.getAs("jt") == null || !fList.contains(row.getAs("jt").toString())){
					return false;
				}
				
				return true;
			}
		});
		 
		 //进行封装 LogEM , 五个行为的单独封装
		 JavaRDD<LogEM> resultRDD_1 = RDD1filter.mapPartitions(new FlatMapFunction<Iterator<Row>, LogEM>() {
			private static final long serialVersionUID = -4162477657967013971L;
			@Override
			public Iterator<LogEM> call(Iterator<Row> iterator) throws Exception {
				
				List<LogEM> returnList = new ArrayList<LogEM>();
				//大循环
				
				while(iterator.hasNext()){
					Row row = iterator.next();
					//创建封装对象
					LogEM logem = new LogEM();
					logem.setId(row.getAs("id") == null ? null : row.getAs("id").toString());
					logem.setCreate_date(row.getAs("create_date") == null ? null : row.getAs("create_date").toString());
					logem.setCustomer_id(row.getAs("customer_id") == null ? null : row.getAs("customer_id").toString());
					logem.setPhone(row.getAs("phone") == null ? null : row.getAs("phone").toString());
					String jt = row.getAs("jt") == null ? null : row.getAs("jt").toString();
					String jc =  row.getAs("jc") == null ? null : row.getAs("jc").toString();
					//jt ==  type ,jc == task, jc ==  type_id  
					logem.setTask(jt);
					logem.setType(jt);
					logem.setType_id(jc);
					logem.setExh(row.getAs("exh") == null ? null : row.getAs("exh").toString());
					logem.setJt(jt);
					logem.setJc(jc);
					//封装商品id
					logem.setProduct_id(row.getAs("product_id") == null ? null : row.getAs("product_id").toString());
					returnList.add(logem);
				}
				return returnList.iterator();
			}
		});
		 //=====================================result_RDD 2============================ 
		 
		 //左面数据集 log
		 //customer_id + 时间毫秒值 + task +  做key 
		 JavaPairRDD<String, LogEM> leftRDD = logDataSet.javaRDD().mapPartitionsToPair(new PairFlatMapFunction<Iterator<Row>, String, LogEM>() {

			private static final long serialVersionUID = -4737763017150873363L;

			@Override
			public Iterator<Tuple2<String, LogEM>> call(Iterator<Row> iterator) throws Exception {
				List<Tuple2<String, LogEM>> returnList = new ArrayList<Tuple2<String, LogEM>>();
				SimpleDateFormat sdf = broadcastSdf.value();
				//大循环
				while(iterator.hasNext())
				{
					Row row = iterator.next();
					
					String customer_id  = row.getAs("customer_id");
					if(customer_id == null || customer_id.toLowerCase().equals("null")){
						customer_id =null;
					}
					
					String create_date = row.getAs("create_date") == null ? null : row.getAs("create_date").toString();
					String task = row.getAs("task") == null ? null : row.getAs("task").toString();
					
					String key = null;
					
					//key
					try{
						key = customer_id + sdf.parse(create_date).getTime() + task;
					}catch(Exception e){
						key = customer_id + "NULL" + task;
					}
					
					//LogEM
					LogEM logEM = new LogEM();
					//+ "id , opean_id, create_date, ip, store_id, customer_id, equipment_type,
					//mac_address, type_id, type, task, eshop_id, order_id, phone  "
					logEM.setId(row.getAs("id") == null ? null : row.getAs("id").toString());
					logEM.setOpean_id(row.getAs("opean_id") == null ? null : row.getAs("opean_id").toString());
					logEM.setCreate_date(create_date);
					logEM.setIp(row.getAs("ip") == null ? null : row.getAs("ip").toString());
					logEM.setStore_id(row.getAs("store_id") == null ? null : row.getAs("store_id").toString());
					logEM.setCustomer_id(customer_id);
					logEM.setEquipment_type(row.getAs("equipment_type") == null ? null : row.getAs("equipment_type").toString());
					
					String mac_address = row.getAs("mac_address");
					if(mac_address == null  || mac_address.toLowerCase().equals("null")){
						mac_address = null;
					}
					logEM.setMac_address(mac_address);
					
					logEM.setType_id(row.getAs("type_id") == null ? null : row.getAs("type_id").toString());
					logEM.setType(row.getAs("type") == null ? null : row.getAs("type").toString());
					logEM.setTask(task);
					logEM.setEshop_id(row.getAs("eshop_id") == null ? null : row.getAs("eshop_id").toString());
					logEM.setOrder_id(row.getAs("order_id") == null ? null : row.getAs("order_id").toString());
					logEM.setPhone(row.getAs("phone") == null ? null : row.getAs("phone").toString());
					//封装商品id
					logEM.setProduct_id(row.getAs("product_id") == null ? null : row.getAs("product_id").toString());
					
					//封装返回值
					Tuple2<String, LogEM> tuple = new Tuple2<String, LogEM>(key , logEM);
					
					returnList.add(tuple);
				}
				
				return returnList.iterator();
			}
		},true);
		 
		 
		 //右面的数据集  mongo
		 //customer_id + 时间毫秒值 + task +  做key 
		 JavaPairRDD<String, HashMap<String, String>> mongoRrightRDD  =  mongoDataSet.javaRDD().filter(new Function<Row, Boolean>() {
			
			private static final long serialVersionUID = -8528366525990641319L;

			@Override
			public Boolean call(Row row) throws Exception {
				//去除五个行为,主线2_2
				List<String> fList = broadcastFiveActionList.value();
				//如果 jt 为空 并且 在五种行为之内,就过滤
				if(row.getAs("jt") == null || fList.contains(row.getAs("jt").toString())){
					return false;
				}
				return true;
			}
		}).mapPartitionsToPair(new PairFlatMapFunction<Iterator<Row>, String, HashMap<String,String>>() {

			private static final long serialVersionUID = 7105356874942987947L;

			@Override
			public Iterator<Tuple2<String, HashMap<String, String>>> call(Iterator<Row> iterator) throws Exception {
				
				List<Tuple2<String, HashMap<String, String>>> returnList = new ArrayList<Tuple2<String, HashMap<String, String>>>();
				SimpleDateFormat sdf = broadcastSdf.value();
				//大循环
				while(iterator.hasNext()){
					Row row = iterator.next();
					
					String customer_id  = row.getAs("customer_id") == null ? null : row.getAs("customer_id").toString();
					String create_date = row.getAs("create_date") == null ? null : row.getAs("create_date").toString();
					String task = row.getAs("task") == null ? null : row.getAs("task").toString();
					String exh =row.getAs("exh") == null ? null : row.getAs("exh").toString();
					String jc =row.getAs("jc") == null ? null : row.getAs("jc").toString();
					String jt =row.getAs("jt") == null ? null : row.getAs("jt").toString();
					
					String key  = customer_id+sdf.parse(create_date).getTime()+task;
					
					HashMap<String,String> returnMap= new HashMap<String,String>();
					returnMap.put("exh", exh);
					returnMap.put("jc", jc);
					returnMap.put("jt", jt);
					
					Tuple2<String, HashMap<String, String>> tuple2 = new Tuple2<String,HashMap<String,String>>(key , returnMap);
					//添加返回值
					returnList.add(tuple2);
				}
				
				return returnList.iterator();
			}
		});;
		 
		//左右数据leftjoin
		JavaPairRDD<String, Tuple2<LogEM, Optional<HashMap<String, String>>>> leftOuterJoinRDD = leftRDD.leftOuterJoin(mongoRrightRDD);

		//进一步封装,返回 logEM对象
		JavaRDD<LogEM> resultRDD_2 = leftOuterJoinRDD.map(new Function<Tuple2<String,Tuple2<LogEM,Optional<HashMap<String,String>>>>, LogEM>() {

			private static final long serialVersionUID = -3833583083231459309L;

			@Override
			public LogEM call(Tuple2<String, Tuple2<LogEM, Optional<HashMap<String, String>>>> tuple) throws Exception {
				
				Tuple2<LogEM, Optional<HashMap<String, String>>> tuple2 = tuple._2;
				
				LogEM logEM = tuple2._1;
				Optional<HashMap<String, String>> optional = tuple2._2;
				
				if(! optional.isPresent()){
					//如果右面mongo数据没有匹配上,就直接返回
					return logEM;
				}else{
					//如果join上了,就给对象加上  exh , jc ,jt 
					HashMap<String, String> map = optional.get();
					
					logEM.setExh(map.get("exh"));
					logEM.setJc(map.get("jc"));
					logEM.setJt(map.get("jt"));
					return logEM;
				}
			}
		});
		 
		 //两个 结果RDD进行合并  
		JavaRDD<LogEM> resultRDD = resultRDD_1.union(resultRDD_2);
		 
		 //写入hive
		 Dataset<Row> createDataFrameRDD = spark.createDataFrame(resultRDD, LogEM.class);
		 try {
			createDataFrameRDD.createTempView("log_tmp_eshop_em");
		} catch (AnalysisException e) {
			System.out.println("log_tmp_eshop_em 临时表创建失败");
			e.printStackTrace();
		}
		  
		 if(isCreate){
			 //有就删除  log_eshop
			 spark.sql("drop table if exists gemini."+tableName+" purge ").count();
			 spark.sql("create table    gemini."+tableName+" as   select * from log_tmp_eshop_em ").count();
		 }else{
			 //写入hive
			 spark.sql("insert into   gemini."+tableName+"  select * from log_tmp_eshop_em ").count();
		 }
		 
		 jsc.close();
		 spark.close();
		 
	}
	
}
