package com.guoan.eslog;

import static org.elasticsearch.spark.rdd.api.java.JavaEsSpark.esRDD;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
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

import com.guoan.pojo.LogW;
import com.guoan.pojo.Log_Collect;
import com.guoan.utils.FileUtil;
import com.guoan.utils.ImpalaPoolUtils;

import net.sf.json.JSONException;
import net.sf.json.JSONObject;
import scala.Tuple2;

/**
  * Description:  行为日志清洗(es数据) (查询方式为拼接sql进行查询)
  * require : cid_phone 表
  * @author lyy  
  * @date 2018年6月5日
 */
public class Wx_log_es_two {
	//es索引库
	private static final String indexPath = "guoanshequ-*";
	private static final String tableName = "log_wx";
	private static final String tableName_F= "log_wx_m_c";
	private static final String collectTableName = "Log_Collect";
	//分区数量
	private static final int partitionNum = 3; 
	private static final String configPath = "conf/config.properties";
	
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
		
		String esIp = FileUtil.getProperties("esIp",configPath);
		String esPort = FileUtil.getProperties("esPort",configPath);
		
		SparkConf conf = new SparkConf();
        conf.set("es.nodes", esIp);
        conf.set("es.port", esPort);
        conf.set("es.scroll.size", "10000");
        conf.set("spark.broadcast.compress", "true");// 设置广播压缩
        conf.set("spark.rdd.compress", "true");      // 设置RDD压缩
        conf.set("spark.io.compression.codec", "org.apache.spark.io.LZFCompressionCodec");
        conf.set("spark.shuffle.file.buffer", "1280k");
        conf.set("es.index.auto.create", "true");
        conf.set("es.index.read.missing.as.empty", "yes");
        conf.set("spark.reducer.maxSizeInFlight", "1024m");
        conf.set("spark.reducer.maxMblnFlight", "1024m");
        
		 //读取测试hive数据
		 SparkSession spark = SparkSession
			      .builder()
			      .config(conf)
			      .enableHiveSupport()
			      .appName("Spark Action_es_wx_two")
			      .getOrCreate();
		 
		 JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
		 jsc.setLogLevel("INFO");
		 
		 //存放mdop,customer的广播变量
		 
		 ImpalaPoolUtils ipu =new  ImpalaPoolUtils();
		 
		 String sql = "select  mdop, customer  from gemini."+tableName_F;
		 Map<Object, Object> m_cMap = ipu.getMap(sql);
		 sql = "select customer , mdop from gemini."+tableName_F;
         Map<Object, Object> c_mMap = ipu.getMap(sql);
         
         //==电话 用户id对照表
         sql = "select id , mobilephone from gemini.view_cid_phone";
         Map<Object,Object> cid_customer = ipu.getMap(sql);
         
		 
		 final Broadcast<Map<Object, Object>> broadcastMC = jsc.broadcast(m_cMap);
		 final Broadcast<Map<Object, Object>> broadcastCM = jsc.broadcast(c_mMap);
		 final Broadcast<Map<Object, Object>> broadcastCidPhone = jsc.broadcast(cid_customer);
		 
		 SimpleDateFormat simpleSdf = new SimpleDateFormat("yyyy-MM-dd");
		 String yesterday = simpleSdf.format(  new Date(new Date().getTime()-24*60*60*1000));
		 
		 
		 StringBuffer query = new StringBuffer();
		  query.append("{"+
				  "  \"_source\": {"+
				  "        \"includes\": [ \"message\"]"+
				  "        },  "+
				  "  \"query\": {"+
				  "    \"bool\": {"+
				  "      \"must\": ["+
				  "        { \"match\": { \"tags\":  \"wxapi\" }},"+
				  "        { \"match\": { \"message\": \"%传递参数%\"}},"+
				  "        {\"match\": {"+
				  "          \"_index\": \"guoanshequ-"+yesterday+"\""+
				  "        }}"+
				  "      ]"+
				  "    }"+
				  "  }"+
				  "}");
		  
		 JavaPairRDD<String, Map<String, Object>> esRDD2 =esRDD(jsc, indexPath ,query.toString());
		 
		 //分区
		 JavaPairRDD<String, Map<String, Object>> repartitionRDD = esRDD2.repartition(partitionNum);
		 
		 //转化输出
		 JavaRDD<String> mapRDD= repartitionRDD.map(new Function<Tuple2<String,Map<String,Object>>, String>() {
			private static final long serialVersionUID = -1182466702036443750L;
			@Override
			public String call(Tuple2<String, Map<String, Object>> tuple) throws Exception {
				Map<String, Object> map = tuple._2;
				String line = map.get("message") == null ? "null" : map.get("message").toString();
				return line;
			}
		  });
		 
		 //过滤,将不含"传递参数" 的行过滤掉
		 JavaRDD<String> filterRDD = mapRDD.filter(new Function<String, Boolean>() {
			private static final long serialVersionUID = 471987061441766908L;
			@Override
			public Boolean call(String line) throws Exception {
				return line.contains("传递参数");
			}
		 });
		  
		 //分割字符串进行封装处理
		 JavaRDD<LogW> resultRDD = filterRDD.mapPartitions(new FlatMapFunction<Iterator<String>, LogW>() {

			private static final long serialVersionUID = 1359555670808979224L;

			@Override
			public Iterator<LogW> call(Iterator<String> iterator) throws Exception {
				
				List<LogW> resultList = new ArrayList<LogW>();
				
				//补map
				Map<Object, Object> MCMap = broadcastMC.value();
				Map<Object, Object> CMMap = broadcastCM.value();
				Map<Object, Object> cid_phone = broadcastCidPhone.value();
				
				//大循环
				A : while(iterator.hasNext()){
					String line = iterator.next();
					
					//根据 "传递参数" 进行分割
					String[] arr1 = line.split("传递参数");
					if(arr1.length <= 1){
						continue A;
					}
					
					LogW logW = new LogW();
					logW.setId((UUID.randomUUID().toString().replace("-", "")));
					//时间
					String create_date = arr1[0].substring(0, 21).replace("[", "").replace("]", "");
					logW.setCreate_date(create_date);
					//Json转化
					try{
						JSONObject fromObject = JSONObject.fromObject(arr1[1].trim());
						String url = fromObject.get("url") == null || "".equals(fromObject.get("url").toString().trim()) ? null : fromObject.get("url").toString();
						logW.setUrl(url);
						String route = fromObject.get("route")  == null || "".equals(fromObject.get("route").toString().trim()) ? null : fromObject.get("route").toString();
						logW.setRoute(route);
						//customer
						String customer= fromObject.get("customer") == null || "".equals(fromObject.get("customer").toString().trim()) ? null : fromObject.get("customer").toString();
						logW.setCustomer_id(customer);
						String phone = cid_phone.get(customer) == null ? null : cid_phone.get(customer).toString();
						logW.setPhone(phone);
						//mdop
						String mdop = fromObject.get("mdop") == null  || "".equals(fromObject.get("mdop").toString().trim())  ? null : fromObject.get("mdop").toString();
						logW.setMdop(mdop);
						logW.setAdtag(fromObject.get("adtag") == null  || "".equals(fromObject.get("adtag").toString().trim())  ? null : fromObject.get("adtag").toString());
						//hearder中的数据
						JSONObject jsonObject = fromObject.getJSONObject("header");
						logW.setUser_agent(jsonObject.get("user-agent") == null  || "".equals(jsonObject.get("user-agent").toString().trim())  ? null : jsonObject.get("user-agent").toString());
						logW.setStoreid(jsonObject.get("storeid") == null  || "".equals(jsonObject.get("storeid").toString().trim())  ? null : jsonObject.get("storeid").toString());
						logW.setFrontid(jsonObject.get("frontid") == null  || "".equals(jsonObject.get("frontid").toString().trim())  ? null : jsonObject.get("frontid").toString());
						logW.setIp(jsonObject.get("x-real-ip") == null  || "".equals(jsonObject.get("x-real-ip").toString().trim())  ? null : jsonObject.get("user-agent").toString());
						//互相补充
					 if(customer == null && mdop != null){
						 	String cus =null;
						    if(MCMap != null){
						    	cus = MCMap.get(mdop) == null ? null : MCMap.get(mdop).toString();
						    }
						    logW.setCustomer_id(cus);
						}else if(mdop == null && customer != null){
							String md = null;
							if(CMMap != null){
								md = CMMap.get(customer) == null ? null : CMMap.get(customer).toString();
							}
							logW.setMdop(md);
						}
					 	
					 	//为记录添加商品id和店铺id
					 	List<LogW> logReturnList  = null;
						if(route != null){
							logReturnList = addProidEshopid(route,url,fromObject,logW);
						}
						
						//加入返回结果集
						if(logReturnList.size()>0){
							resultList.addAll(logReturnList);
						}else{
							resultList.add(logW);
						}
						
					}catch(JSONException e){
						System.out.println(arr1[1]);
					}catch(Exception e){
						e.printStackTrace();
					}
				}
				return resultList.iterator();
			}
		});
		 
		 //封装成 Log_Collect
		 JavaRDD<Log_Collect> resultRDD2 = resultRDD.mapPartitions(new FlatMapFunction<Iterator<LogW>, Log_Collect>() {

			private static final long serialVersionUID = 3987564987756619993L;

			@Override
			public Iterator<Log_Collect> call(Iterator<LogW> iterator) throws Exception {
				
				List<Log_Collect> returnList = new ArrayList<Log_Collect>();
				//大循环
				while(iterator.hasNext())
				{
					LogW logW = iterator.next();
					
					Log_Collect Log_Collect = getLog_Collect(logW);
					returnList.add(Log_Collect);
				}
				
				return returnList.iterator();
			}
		});
		 
		 //写入hive == log_wx
		 Dataset<Row> createDataFrameRDD = spark.createDataFrame(resultRDD, LogW.class);
		 try {
			createDataFrameRDD.createTempView("log_tmp_wx");
		} catch (AnalysisException e) {
			System.out.println("log_tmp_wx 临时表创建失败");
			e.printStackTrace();
		}
		 
		 if(isCreate){
			 //有就删除
			 spark.sql("drop table if exists gemini."+tableName+" purge ");
			 spark.sql("create table    gemini."+tableName+"  as   select * from log_tmp_wx").count();
		 }else{
			 //写入hive
			 spark.sql("insert into   gemini."+tableName+"    select * from log_tmp_wx").count();
		 }
		
		//写入hive == Log_Collect
		 Dataset<Row> createDataFrameRDD2 = spark.createDataFrame(resultRDD2, Log_Collect.class);
		 try {
			 createDataFrameRDD2.createTempView("log_tmp_collect");
			} catch (AnalysisException e) {
				System.out.println("log_tmp_collect 临时表创建失败");
				e.printStackTrace();
			}
		 
		 //有就删除
		 spark.sql("drop table if exists gemini."+collectTableName+" purge ").count();
		 spark.sql("create table  gemini."+collectTableName+"  as   select * from log_tmp_collect").count();
		 
		 //写入hive
//		 spark.sql("insert into   gemini."+collectTableName+"    select * from log_tmp_middle").count();
		
		spark.close();
		jsc.close();	
		
	}
	/**
	  * Title: addProidEshopid
	  * Description: 补充商品id和E店id
	  * @return
	 * @throws CloneNotSupportedException 
	 */
	public static List<LogW> addProidEshopid(String route,String url,JSONObject fromObject , LogW logW ) throws CloneNotSupportedException{
		List<LogW> retrunList = new ArrayList<LogW>();

		//添加普通商品id
		if(		"积分商品详情".equals(route) ||
				"评价列表".equals(route) ||
				"分享商品详情".equals(route) ||
				"商品详情".equals(route) ||
				"新立即购买".equals(route) ||
				"购物车删除 ".equals(route) ||
				"积分商品评价列表".equals(route)
			){
			//解析url
			if(url != null && url.length()>0 ){
				String[] urlArr = url.split("/");
				for (String param : urlArr) {
					//说明是uuid , 有可能是多个参数
					if(param.length() == 32){
						LogW LogWClone = (LogW) logW.clone();
						LogWClone.setProduct_id(param);
						retrunList.add(LogWClone);
					}
				}
			}
		}
		//添加E店id
		if(		"取消收藏".equals(route) ||
				"分享店铺详情".equals(route) ||
				"店铺详情".equals(route)
			){
			if(url != null && url.length()>0){
				String[] urlArr = url.split("/");
				for (String param : urlArr) {
					//说明是uuid , 有可能是多个参数
					if(param.length() == 32){
						LogW LogWClone = (LogW) logW.clone();
						LogWClone.setEshop_id(param);
						retrunList.add(LogWClone);
					}
				}
			}
		}
		
		//特殊商品id
		//特殊店铺id
		if("添加购物车".equals(route) || "再来一单".equals(route) ){
			String body = fromObject.get("Body") == null || "".equals(fromObject.get("Body").toString().trim()) ? null : fromObject.get("Body").toString();
			if(body != null && body.length()>0){
				String[] bodyArr = body.split("\"");
				for (String proid : bodyArr) {
					if(proid.length() == 32){
						LogW LogWClone = (LogW) logW.clone();
						LogWClone.setProduct_id(proid);
						retrunList.add(LogWClone);
					}
				}
			}
			
		}else if("添加收藏".equals(route)){
			//店铺收藏
			String body = fromObject.get("Body") == null || "".equals(fromObject.get("Body").toString().trim()) ? null : fromObject.get("Body").toString();
			if(body != null && body.length()>0){
				String[] bodyArr = body.split("\"");
				for (String eshopid : bodyArr) {
					if(eshopid.length() == 32){
						LogW LogWClone = (LogW) logW.clone();
						LogWClone.setEshop_id(eshopid);
						retrunList.add(LogWClone);
					}
				}
			}
		}
		
		return retrunList;
	}
	
	
	
	/**
	  * Title: getLog_Collect
	  * Description: 获取整合大表的中间表
	  * @return
	 */
	public static Log_Collect getLog_Collect(LogW logw){
		Log_Collect lm = new Log_Collect();
//		private String id;
		lm.setId(UUID.randomUUID().toString().replace("-", ""));
//		private String token;
		lm.setToken(null);
//		private String customer_id;
		lm.setCustomer_id(logw.getCustomer_id());
//		private String phone;
		lm.setPhone(logw.getPhone());
//		private String behavior;
		lm.setBehavior(logw.getUrl());
//		private String behavior_param;
		lm.setBehavior_param(logw.getRoute());
//		private String behavior_name;
		lm.setBehavior_name(logw.getRoute());
//		private String create_date;
		String create_date = logw.getCreate_date();
		if(create_date != null && !"".equals(create_date) && !"null".equals(create_date)){
			lm.setSimple_date(create_date.substring(0, 10));
		}
		lm.setCreate_date(create_date);
//		private String two_behavior;
		lm.setTwo_behavior(null);
//		private String two_behavior_name;
		lm.setTwo_behavior_name(null);
//		private String store_id;
		lm.setStore_id(logw.getStoreid());
//		private String eshop_id;
		lm.setEshop_id(logw.getEshop_id());
//		private String message;
		lm.setMessage(logw.getAdtag());
//		private String payPlatform;
		lm.setPayPlatform(null);
//		private String order_id;
		lm.setOrder_id(null);
//		private String group_id;
		lm.setGroup_id(null);
//		private String log_type;
		lm.setLog_type("wx");
		//product_id
		lm.setProduct_id(logw.getProduct_id());
		return lm;
	}
	
	@Test
	public void test01(){
		
	}
}

