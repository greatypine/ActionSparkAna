package com.guoan.eslog;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;
import java.util.UUID;

import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.esotericsoftware.minlog.Log;
import com.guoan.pojo.Customer_Visit;

import scala.Tuple2;

/**
  * Description:  统计30天的用户访问数
  * require : log_final
  * @author lyy  
  * @date 2018年6月5日
 */
public class Log_visit_num {
	
	private static final String getTableName = "datacube_kudu.log_final";
	private static final String saveTableName = "gemini.customer_visit_num";
	private static final int partitionNum = 6;
	//业务制定,超过一分钟的两个行为才算点击量
	private static final long interval = 60*1000;
	
	public static void main(String[] args) throws Exception {
		
		System.setProperty("user.name", "hdfs");

		 //读取测试hive数据
		 SparkSession spark = SparkSession
			      .builder()
			      .enableHiveSupport()
			      .appName("Spark Log_visit_num")
			      .getOrCreate();
		 
		 JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
		 jsc.setLogLevel("INFO");
		 
		 SimpleDateFormat simpleSdf = new SimpleDateFormat("yyyy-MM-dd");
		 long  longTime = 30*24*60*60*1000L;
		 String thirtyDay = simpleSdf.format(  new Date(new Date().getTime()-longTime));
		 
		 final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		 final Broadcast<SimpleDateFormat> broadcast_sdf = jsc.broadcast(sdf);
		 
		 String sql = "select customer_id , create_date ,simple_date from "+getTableName+" where 1=1  "
		 		+ " and simple_date > '"+thirtyDay+"'"
		 		+ " and behavior_name is not null "
		 		+ " and customer_id is not null "
		 		+ " and customer_id != 'fakecustomerformicromarket000002' ";
		 
		 Dataset<Row> dataset = spark.sql(sql);
		 JavaRDD<Row> javaRDD = dataset.javaRDD();
		 //生成 customer_id == row 的key,
		 JavaPairRDD<String, Row> pairRDD = javaRDD.repartition(partitionNum).mapPartitionsToPair(new PairFlatMapFunction<Iterator<Row>, String, Row>() {

			private static final long serialVersionUID = -4917712721779346892L;

			@Override
			public Iterator<Tuple2<String, Row>> call(Iterator<Row> iterator) throws Exception {
				List<Tuple2<String, Row>> returnList= new ArrayList<Tuple2<String, Row>>();
				
				//大循环
				while(iterator.hasNext()){
					Row row = iterator.next();
					Tuple2<String, Row> tuple2 = new Tuple2<String, Row>(row.getAs("customer_id")+"" , row);
					returnList.add(tuple2);
				}
				
				return returnList.iterator();
			}
		});
		 
		 //重新分区,相同的key分到同一分区
		 JavaPairRDD<String, Row> partitionByRDD = pairRDD.partitionBy(new Partitioner() {
			private static final long serialVersionUID = 1L;
			@Override
			public int numPartitions() {
				return partitionNum;
			}
			@Override
			public int getPartition(Object key) {
				int hashCode = key.hashCode();
				//因为hashCode可能产生负数,所以取绝对值
				int absPartitions = Math.abs(hashCode % partitionNum);
				return absPartitions;
			}
		}).cache();
		 
		 //group by key,分析其浏览次数以及 最后浏览时间
		 JavaRDD<Customer_Visit> resultRDD = partitionByRDD.groupByKey().mapPartitions(new FlatMapFunction<Iterator<Tuple2<String,Iterable<Row>>>, Customer_Visit>() {

			private static final long serialVersionUID = 1100735169445703679L;

			@Override
			public Iterator<Customer_Visit> call(Iterator<Tuple2<String, Iterable<Row>>> iterator) throws Exception {
				
				SimpleDateFormat sdf = broadcast_sdf.value();
				
				List<Customer_Visit> returnList = new ArrayList<Customer_Visit>();
				
				//大循环.一次循环一个用户
				while(iterator.hasNext()){
					Tuple2<String, Iterable<Row>> tuple2 = iterator.next();
					Customer_Visit cv = new Customer_Visit();
					String customer_id = tuple2._1;
					
					Iterator<Row> iterator2 = tuple2._2.iterator();
					int visit_num = 0;
					//两个功能
					//(1) 将时间进行排序
					//(2) 相同的时间可以第一次去重
					TreeMap<String,String> treeMap = new TreeMap<String,String>();
					
					//小循环
					while(iterator2.hasNext()){
						Row row = iterator2.next();
						treeMap.put(row.getAs("create_date"),"1" );
					}
					
					//在这判断是否时间间隔超过1分钟
					Iterator<String> iterator3 = treeMap.keySet().iterator();
					String oneDate = null;
					while(iterator3.hasNext()){
						if(oneDate == null){
							oneDate = iterator3.next();
						}else{
							String twoDate = iterator3.next();
							//判断间隔是否是否 超过一分钟
							long one_time = 0L;
							long two_time =0L;
							try{
								 one_time = sdf.parse(oneDate).getTime();
								 two_time = sdf.parse(twoDate).getTime();
								 if((two_time - one_time) >= interval){
									 visit_num++;
								 }
							}catch(ParseException e){
								Log.error("日期转换出现问题!!!");
							}
							oneDate= twoDate;
						}
					}
					
					cv.setId(UUID.randomUUID().toString().replace("-", ""));
					cv.setCustomer_id(customer_id);
					cv.setVisit_num(visit_num);
					cv.setLast_login(treeMap.lastKey());
					
					returnList.add(cv);
				}
				return returnList.iterator();
			}
		});
		 
		 //写入hive
		 Dataset<Row> createDataFrameRDD = spark.createDataFrame(resultRDD, Customer_Visit.class);
		 try {
				createDataFrameRDD.createTempView("spark_customer_visit");
			 } catch (AnalysisException e) {
				 Log.error("spark_customer_visit 临时表创建失败");
				 e.printStackTrace();
			 }
		 
		 //有就删除
		 spark.sql("drop table if exists "+saveTableName+" purge ").count();
		 //写入hive
		 spark.sql("create table "+saveTableName+" as select * from spark_customer_visit").count();
		 
		spark.close();
		jsc.close();	
		
	}
	
	
}

