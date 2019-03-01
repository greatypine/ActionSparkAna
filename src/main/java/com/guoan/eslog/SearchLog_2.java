package com.guoan.eslog;

import static org.elasticsearch.spark.rdd.api.java.JavaEsSpark.esRDD;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.guoan.pojo.Log_Search;
import com.guoan.utils.ImpalaPoolUtils;

import net.sf.json.JSONException;
import net.sf.json.JSONObject;
import scala.Tuple2;

public class SearchLog_2 {

	private static final String tableName = "t_search_log";
	//分区数量
	private static final int partitionNum = 3; 
	private static final String indexPath = "guoanshequ-*";
	static Logger logger = LoggerFactory.getLogger(SearchLog_2.class);
	public static void main(String[] args) throws Exception {
		
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
		
		SparkConf conf = new SparkConf();
        conf.set("es.nodes", "10.10.30.8");
        conf.set("es.port", "9200");
        conf.set("es.scroll.size", "10000");
        conf.set("spark.rdd.compress", "true");      // 设置RDD压缩
        conf.set("spark.io.compression.codec", "org.apache.spark.io.LZFCompressionCodec");
        conf.set("es.index.auto.create", "true");
        conf.set("es.index.read.missing.as.empty", "yes");
        
		 //读取测试hive数据
		 SparkSession spark = SparkSession
			      .builder()
			      .master("local")
			      .config(conf)
			      .enableHiveSupport()
			      .appName("Spark search_es")
			      .getOrCreate();

		 JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
		 jsc.setLogLevel("INFO");
		 SimpleDateFormat yesterdataySdf = new SimpleDateFormat("yyyy-MM-dd");
		 String yesterday = yesterdataySdf.format(  new Date(new Date().getTime()-24*60*60*1000));
		 
		 SimpleDateFormat dateSdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		 final Broadcast<SimpleDateFormat> broadcast_sdf = jsc.broadcast(dateSdf);
		 
		 //商品id和商品名称的map
		 ImpalaPoolUtils ipu =new  ImpalaPoolUtils();
         String sql =  "select id , content_name from gabdp_user.view_product_id_name limit 10"
         		+ " ";
         Map<Object, Object> product_id_name_map = ipu.getMap(sql);
         final Broadcast<Map<Object, Object>> broadcast_product_id_name_map = jsc.broadcast(product_id_name_map);
		 
		 
         StringBuffer query = new StringBuffer();
		 query.append(
				  "{"
				  +"  \"_source\": {"
				  +"        \"includes\": [ \"message\"]"
				  +"        },"
				  +"  "
				  +"  \"query\": {"
				  +"    \"bool\": {"
				  +"      \"must\": ["
				  +"        { \"match\": { \"tags\":  \"gsearch_analysis\" }},"
				  +"        {\"match\": {"
				  +"          \"source\": \"/tomcat/logs/gsearch_analysis."+yesterday+".log\""
				  +"        }}"
				  +"      ]"
				  +"    }"
				  +"  }"
				  +"}");
		  
		 JavaPairRDD<String, Map<String, Object>> esRDD2 =
				 esRDD(jsc, indexPath ,query.toString());
		 
		 //分区
		 JavaPairRDD<String, Map<String, Object>> repartitionRDD = esRDD2.repartition(partitionNum);
		 
		 JavaRDD<String> mapRDD= repartitionRDD.map(new Function<Tuple2<String,Map<String,Object>>, String>() {
				private static final long serialVersionUID = -1182466702036443750L;
				@Override
				public String call(Tuple2<String, Map<String, Object>> tuple) throws Exception {
					Map<String, Object> map = tuple._2;
					String line = map.get("message") == null ? "null" : map.get("message").toString();
					return line;
				}
			  });
		 
		 //一个message里面可能有多条数据,根据换行符进行分割
		 JavaRDD<String> flatMapRDD = mapRDD.flatMap(new FlatMapFunction<String, String>() {

			private static final long serialVersionUID = -881867999198841062L;

			@Override
			public Iterator<String> call(String line) throws Exception {
				String[] lines = line.split("\\n");
				return Arrays.asList(lines).iterator();
			}
		});
		 
		 //封装产生结果集
		 JavaRDD<Log_Search> resultRDD = flatMapRDD.mapPartitions(new FlatMapFunction<Iterator<String>, Log_Search>() {

				private static final long serialVersionUID = 1570154238203739202L;

				@Override
				public Iterator<Log_Search> call(Iterator<String> iterator) throws Exception {
					SimpleDateFormat sdf = broadcast_sdf.value();
					Map<Object, Object> productMap = broadcast_product_id_name_map.value();
					
					List<Log_Search> resultList = new ArrayList<Log_Search>();
					
					while(iterator.hasNext()){
						String line = iterator.next();
						String[] arr = line.split("-\\{");
						
						if(arr.length >1){
							Log_Search logS = new Log_Search();
							
							String jsonStr= "{"+arr[1];
							try{
								JSONObject fromObject = JSONObject.fromObject(jsonStr.trim());
								
								logS.setId(UUID.randomUUID().toString().replace("-", ""));
								logS.setCustomer_id(fromObject.get("customerId") == null ? null :fromObject.get("customerId").toString());
								logS.setMobilephone(fromObject.get("mobilephone") == null ? null :fromObject.get("mobilephone").toString());
								logS.setKey(fromObject.get("keyword") == null ? null :fromObject.get("keyword").toString());
								//时间
								String timeStr = fromObject.get("time") == null ? null :fromObject.get("time").toString();
								if(timeStr != null && timeStr.length()>0){
									String create_time = sdf.format(new Date(Long.valueOf(timeStr)));
									logS.setCreate_time(create_time);
									logS.setSimple_date(create_time.substring(0, 10));
								}
								
								//门店id
								String storeIdStr =fromObject.get("storeId") == null ? null :fromObject.get("storeId").toString() ;
								
								if(storeIdStr !=null && storeIdStr.length() >0){
									String[] storeArr = storeIdStr.split(",");
									String store_id = null;
									String other_store_id = null;
									for (String store_str : storeArr) {
										if(store_str.contains("F_")){
											logS.setFront_store_id(store_str.trim().replace("F_", ""));
											other_store_id = store_str.trim().replace("F_", "");
										}else if (store_str.contains("C_")){
											logS.setCloud_store_id(store_str.trim().replace("C_", ""));
											other_store_id = store_str.trim().replace("C_", "");
										}else{
											store_id = store_str.trim();
										}
									}
									if(store_id == null ){
										store_id = other_store_id;
									}
									logS.setStore_id(store_id);
								}
								
								//返回的商品id,每一个产生一条记录
								String productStr = fromObject.get("proId") == null ? null :fromObject.get("proId").toString() ;
								
								String[] productArr = productStr.split(",");
								if(productArr.length>0){
									
									
									String productNames = "";
									String productIds = "";
									//有返回的就进行拆解,每个返回值封装成一个对象
									for (String product_id : productArr) {
										if(product_id.length()>0){
											
											productIds +=product_id+",";
											String productName = productMap.get(product_id) == null ? null :productMap.get(product_id).toString();
											if(productName != null && productName.length()>0){
												productNames+= productName+",";
											}
										}
									}
									if(productNames!= null && productNames.length()>0){
										logS.setProduct_name(productNames.substring(0, productNames.length()-1));
									}
									if(productIds.length()>0){
										logS.setProduct_id(productIds.substring(0, productIds.length()-1));
									}
								}
								//添加到返回的集合类
								resultList.add(logS);
								
							}catch(JSONException e){
								 logger.error("JSONException   "+jsonStr);
							}catch(Exception e){
								e.printStackTrace();
								 logger.error(jsonStr);
							} 
						}
					}
					return resultList.iterator();
				}
			});
		 
		 //写入hive
		 Dataset<Row> createDataFrameRDD = spark.createDataFrame(resultRDD, Log_Search.class);
		 try {
			createDataFrameRDD.createTempView("log_tmp_search");
		 } catch (AnalysisException e) {
			System.out.println("log_tmp_search 临时表创建失败");
			e.printStackTrace();
		 }
		 
		 if(isCreate){
			 //有就删除
			 spark.sql("drop table if exists default."+tableName+" purge ").count();
			 //写入hive
			 spark.sql("create table default."+tableName+" stored as parquet as select id, customer_id, product_id, product_name, key, create_time, store_id, front_store_id, cloud_store_id, mobilephone  , simple_date from log_tmp_search").count();
		 }else{
			 //写入hive
			 spark.sql("insert overwrite table   default."+tableName+
					 " partition(simple_date)  select id, customer_id, product_id, product_name, key, create_time, store_id, front_store_id, cloud_store_id, mobilephone  , simple_date from log_tmp_search  "
					 + " distribute by simple_date").count();
		 }
		
		 
		 jsc.close();
		 spark.close();
	}
	
	
	@Test
	public void test01(){
		 String timeStr  = "1544285529600";
		 SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		 String create_time = sdf.format(new Date(Long.valueOf(timeStr)));
		 System.out.println(create_time);
	}
	


}
