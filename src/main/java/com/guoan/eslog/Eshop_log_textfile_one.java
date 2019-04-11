package com.guoan.eslog;

import static org.elasticsearch.spark.rdd.api.java.JavaEsSpark.esRDD;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.guoan.utils.DateUtils;
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

import com.alibaba.fastjson.JSON;
import com.guoan.pojo.LogE;
import com.guoan.utils.FileUtil;
import com.guoan.utils.ImpalaPoolUtils;

import scala.Tuple2;

/**
  * Description:  行为日志清洗 es数据 (查询方式为拼接sql进行查询)
  * require : cid_phone 表
  * @author lyy  
  * @date 2018年6月5日
 */

public class Eshop_log_textfile_one {
	
	private static final String indexPath = "guoanshequ-*";
	private static final String tableName = "log_eshop_one";
	//分区数量
	private static final int partitionNum = 3; 
	private static final String configPath = "conf/config.properties";
	
	public static void main(String[] args) throws Exception {
		
		System.setProperty("user.name", "hdfs");
		
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
        // 参数说明：该参数用于设置shuffle read task的buffer缓冲大小，而这个buffer缓冲决定了每次能够拉取多少数据。
        // 调优建议：如果作业可用的内存资源较为充足的话，可以适当增加这个参数的大小（比如96m），从而减少拉取数据的次数，也就可以减少网络传输的次数，
        // 进而提升性能。在实践中发现，合理调节该参数，性能会有1%~5%的提升。
        conf.set("spark.reducer.maxSizeInFlight", "1024m");
        // 未找到
        conf.set("spark.reducer.maxMblnFlight", "1024m");
        
        // Kryo:快速、高效的序列化框架,序列化后的大小比Java序列化小，速度比Java快
        conf.registerKryoClasses(new Class[]{net.sf.json.JSONObject.class, java.util.List.class, java.util.Map.class, java.util.ArrayList.class, java.util.HashMap.class});
		
		 //读取测试hive数据
		 SparkSession spark = SparkSession
			      .builder()
			      .config(conf)
			      .enableHiveSupport()
			      .appName("Spark Action_es_eshop_one")
			      .getOrCreate();
		 
		 JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
		 jsc.setLogLevel("INFO");
		 
		 //SimpleDateFormat simpleSdf = new SimpleDateFormat("yyyy-MM-dd");
		 //String yesterday = simpleSdf.format(  new Date(new Date().getTime()-24*60*60*1000));
		String yesterday = "";
		if(args.length >0){
			yesterday = DateUtils.getDate(args[0]);
		}else{
			yesterday = DateUtils.getDate(null);
		}
		 
		 //加载电话号码和用户id的map
		 ImpalaPoolUtils ipu =new  ImpalaPoolUtils();
         String sql = "select mobilephone , id from gemini.view_cid_phone "
         		+ " ";
         Map<Object, Object> cid_phone_map = ipu.getMap(sql);
         final Broadcast<Map<Object, Object>> cidPhoneMapBroadcast = jsc.broadcast(cid_phone_map);
		 
         //jt对应的task
         Map<String,String> jt2task = new HashMap<String,String>();
         jt2task.put("exhibition", "getexhibition");
         jt2task.put("groupon", "proinfo");
         jt2task.put("sku", "proinfo");
         jt2task.put("eshop", "eshopinfo");
         jt2task.put("emall", "emallprolist");
         jt2task.put("group", "tagprolist");
         jt2task.put("coupon_center", "intProList");
         jt2task.put("groupSku", "intProList");
         final Broadcast<Map<String, String>> jt2TaskBroadcast = jsc.broadcast(jt2task);
         
         
         StringBuffer query = new StringBuffer();
		  
		  query.append("{"+
				  "  \"_source\": {"+
				  "        \"includes\": [ \"message\"]"+
				  "        },"+
				  "  "+
				  "  \"query\": {"+
				  "    \"bool\": {"+
				  "      \"must\": ["+
				  "        { \"match\": { \"tags\":  \"edian\" }},"+
				  "        {\"match\": {"+
				  "          \"source\": \"/home/wwwroot/edian.guoanshequ.com/htdocs/logs/phplog20180613.log\""+
				  "        }},"+
				  "		{\"match\": {"+
				  "          \"_index\": \"guoanshequ-"+yesterday+"\""+
				  "        }}"+
				  "   "+
				  "      ]"+
				  "    }"+
				  "  }"+
				  "}");
		  
		 JavaPairRDD<String, Map<String, Object>> esRDD2 =
				 esRDD(jsc, indexPath ,query.toString());
		 
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
		 
		 //一个message里面可能有多条数据,根据换行符进行分割
		 JavaRDD<String> flatMapRDD = mapRDD.flatMap(new FlatMapFunction<String, String>() {

			private static final long serialVersionUID = -881867999198841062L;

			@Override
			public Iterator<String> call(String line) throws Exception {
				String[] lines = line.split("\\n");
				return Arrays.asList(lines).iterator();
			}
		});
		 
		  
		 //过滤
		 JavaRDD<String> filterRDD = flatMapRDD.filter(new Function<String, Boolean>() {
			
			private static final long serialVersionUID = 471987061441766908L;

			@Override
			public Boolean call(String line) throws Exception {
				//一行数据过少
				if(line.length() <20){
					return false;
				}
				//时间都没有的就直接过滤掉
				String[] arr1 = line.split("customer_android");
				String[] arr2 = line.split("customer_ios");
				if(arr1.length >1 && arr1[0].length() < 19){
					return false;
				}else if(arr2.length >1 && arr2[0].length() < 19){
					return false;
				}
				return true;
			}
		});
         
         
         //读取生成的本地文件
		 JavaRDD<LogE> resultRDD = filterRDD.mapPartitions(new FlatMapFunction<Iterator<String>, LogE>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<LogE> call(Iterator<String> iterator) throws Exception {
				
				Map<Object, Object> cid_phone_map_bro = cidPhoneMapBroadcast.value();
				Map<String, String> jt2TaskMap = jt2TaskBroadcast.value();
				
				ArrayList<LogE> resultList = new ArrayList<LogE>();
				//循环
				A : while(iterator.hasNext()){
					String line = iterator.next();
					
					if(line.length()<10){
						continue A;
					}
					//将时间截掉
					String substring = line.substring(20,line.length());
					
					String[] arr1 = substring.split("customer_android");
					String[] arr2 = substring.split("customer_ios");
					
					if(arr1.length >1 ){
						List<LogE> logEList =  getlogE(arr1 , "customer_android" );
						//有需要将记录拆开的情况
						for(LogE log : logEList){
							//userActSta , mongo记录给phone添加上customer_id 
							if("userActSta".equals(log.getTask()) || "mongo_log".equals(log.getLog_type())){
								if(log.getPhone()!= null && !"".equals(log.getPhone()) && log.getCustomer_id() == null ){
									
									try{
										log.setCustomer_id(cid_phone_map_bro.get(log.getPhone()) == null ? null : cid_phone_map_bro.get(log.getPhone()).toString());
									}catch(NullPointerException e){
										System.out.println(log);
									}
								}
							}
							if("mongo_log".equals(log.getLog_type())){
								//设置上task
								log.setTask( jt2TaskMap.get(log.getJt()) == null ?log.getJt() : jt2TaskMap.get(log.getJt()));
							}
							
							//添加到结果集
							resultList.add(log);
						}//end if
					}
					
					if(arr2.length >1){
						List<LogE> logEList =  getlogE(arr2 , "customer_ios" );
						for(LogE log : logEList){
							//userActSta 给phone添加上customer_id
							if("userActSta".equals(log.getTask()) || "mongo_log".equals(log.getLog_type())){
								if(log.getPhone()!= null && !"".equals(log.getPhone()) && log.getCustomer_id() == null ){
									
									try{
										log.setCustomer_id(cid_phone_map_bro.get(log.getPhone()) == null ? null : cid_phone_map_bro.get(log.getPhone()).toString());
									}catch(NullPointerException e){
										System.out.println(log);
									}
								}
							}
							if("mongo_log".equals(log.getLog_type())){
								//设置上task
								log.setTask( jt2TaskMap.get(log.getJt()) == null ?log.getJt() : jt2TaskMap.get(log.getJt()));
							}
							//添加到结果集
							resultList.add(log);
						}
					}//end else if
				}
				return resultList.iterator();
			}
		});
		 
		 //写入hive
		 Dataset<Row> createDataFrameRDD = spark.createDataFrame(resultRDD, LogE.class);
		 try {
			createDataFrameRDD.createTempView("log_tmp_eshop");
		 } catch (AnalysisException e) {
			System.out.println("log_tmp 临时表创建失败");
			e.printStackTrace();
		 }
		 
		 //有就删除
		 spark.sql("drop table if exists gemini."+tableName+" purge ").count();
		 //写入hive
		 spark.sql("create table gemini."+tableName+" as select * from log_tmp_eshop").count();
		 
		spark.close();
		jsc.close();			
	}
	
	/**
	  * Title: getlogE 
	  * Description: 封装Log
	  * @param line
	  * @return
	 */
	public static List<LogE>  getlogE(String[] arr1  ,String equipment_type  ){
		
		//包含logE和logM的日志
		List<LogE> resultList=  new ArrayList<LogE>();
		
		LogE logE = new LogE();
		logE.setLog_type("all_log");
		
		logE.setId(UUID.randomUUID().toString().replace("-", ""));
		logE.setOpean_id(null);
		
		logE.setEquipment_type(equipment_type);
		//封装门店id, IP, customer_id
		String[] splitArr = arr1[0].trim().split(" ");
		if(splitArr.length == 3){
			logE.setIp("null".equals(splitArr[0])? null : splitArr[0].trim());
			logE.setStore_id("null".equals(splitArr[1])? null : splitArr[1].trim());
			logE.setCustomer_id("null".equals(splitArr[2])? null : splitArr[2].trim());
		}else if (splitArr.length == 2){
			logE.setIp("null".equals(splitArr[0])? null : splitArr[0].trim());
			logE.setStore_id(null);
			logE.setCustomer_id(null);
		}else{
			//说明格式不对,直接设为NULL
			logE.setIp(null);
			logE.setStore_id(null);
			logE.setCustomer_id(null);
		}
		//mac 地址
		String macAddress = arr1[1].substring(arr1[1].lastIndexOf("}")+1,arr1[1].length() ).trim();
		
		logE.setMac_address(macAddress);
		//json字符串
		String jsonStr = arr1[1].trim().substring(0,arr1[1].trim().lastIndexOf("}")+1).trim();
		
		try{
			//转化可能出现问题
			@SuppressWarnings("unchecked")
			HashMap<String,Object> map = JSON.parseObject(jsonStr, HashMap.class);
			//task
			String task = map.get("task") == null ? null : map.get("task").toString();
			logE.setTask(task);
			//eshop_id
			if(map.get("shoppe_id") != null){
				logE.setEshop_id(map.get("shoppe_id").toString());
			}else if (map.get("eshop_id") != null){
				logE.setEshop_id(map.get("eshop_id").toString());
			}else{
				logE.setEshop_id(null);
			}
			//时间戳
			String requestTimestamp = map.get("requestTimestamp") == null ? null : map.get("requestTimestamp").toString();
			logE.setCreate_date(requestTimestamp);
			//设置电话号码
			logE.setPhone(map.get("phone") == null ? null : map.get("phone").toString());
			
			//type_id
			//type
			switch(task){
				case "cateprolist":
					logE.setType_id(map.get("category_id")== null ? null : map.get("category_id").toString());
					logE.setType("category_id");
					break;
				case "getexhibition":
					logE.setType_id(map.get("exb_id") == null ? null : map.get("exb_id").toString());
					logE.setType("exb_id");
					break;
				case "proinfo":
					logE.setType_id(map.get("id") == null ? null : map.get("id").toString());
					logE.setType("id");
					logE.setProduct_id(map.get("id") == null ? null : map.get("id").toString());
					break;
				case "newAppOrder":
					logE.setType_id(map.get("group_id")== null ? null : map.get("group_id").toString());
					logE.setType("group_id");
					break;
				case "eshopprolist":
					logE.setType_id(map.get("category_id")== null ? null : map.get("category_id").toString());
					logE.setType("category_id");
					logE.setEshop_id(map.get("eshop_id")== null ? null : map.get("eshop_id").toString());
					break;
				case "add":
					logE.setType_id(map.get("pid") == null ? null : map.get("pid").toString());
					logE.setType("pid");
					logE.setProduct_id(map.get("pid") == null ? null : map.get("pid").toString());
					break;
				case "edit":
					logE.setType_id(map.get("uid") == null ? null : map.get("uid").toString());
					logE.setType("uid");
					logE.setProduct_id(map.get("uid") == null ? null : map.get("uid").toString());
					break;
				case "del":
					logE.setType_id(map.get("product_id") == null ? null : map.get("product_id").toString());
					logE.setType("product_id");
					logE.setProduct_id(map.get("product_id") == null ? null : map.get("product_id").toString());
					break;
				case "emallprolist":
					logE.setType_id(map.get("category_id") == null ? null : map.get("category_id").toString());
					logE.setType("category_id");
					break;
				case "commentlist":
					logE.setType_id(map.get("pro_id") == null ? null : map.get("pro_id").toString());
					logE.setType("pro_id");
					logE.setProduct_id(map.get("pro_id") == null ? null : map.get("pro_id").toString());
					break;
				case "sele":
					logE.setType_id(map.get("group_id") == null ? null : map.get("group_id").toString());
					logE.setType("group_id");
					break;
				case "collect":
					logE.setType_id(map.get("id") == null ? null : map.get("id").toString());
					logE.setType("id");
					logE.setProduct_id(map.get("id") == null ? null : map.get("id").toString());
					break;
				case "emptyInvalidPro":
					logE.setType_id(map.get("id")== null ? null : map.get("id").toString());
					logE.setType("id");
					break;
				case "arrivalWarn":
					logE.setType_id(map.get("pro_id")== null ? null : map.get("pro_id").toString());
					logE.setType("pro_id");
					break;
				case "eshopProSearch":
					logE.setType_id(map.get("id")== null ? null : map.get("id").toString());
					logE.setType("id");
					logE.setEshop_id("id");
					break;
				case "appOrder":
					logE.setType_id(map.get("group_id") == null ?null : map.get("group_id").toString());
					logE.setType("group_id");
					break;
				case "thirdPartTime":
					logE.setType_id(map.get("pid") == null ? null : map.get("pid").toString());
					logE.setType("pid");
					break;
				case "certificate":
					logE.setType_id(map.get("id")== null ? null : map.get("id").toString());
					logE.setType("id");
					break;
				//产生订单的行为
				//againorder , directBuy task事件会有订单产生,关联其订单id
				case "directBuy":
					logE.setType_id(map.get("pro_id") == null ? null : map.get("pro_id").toString());
					logE.setType("pro_id");
					logE.setProduct_id(map.get("pro_id") == null ? null : map.get("pro_id").toString());
					//根据用户id和时间进行查询,时间往后推 5秒(网络延迟)
					break;
				//加上phone
				case "userActSta":
					
					String jsonData = map.get("data").toString();
					try{
						ArrayList<?> dataList = JSON.parseObject(jsonData, ArrayList.class);
						String getPhone = null;
						if(dataList.size()>= 0 ){
							
							String sigletag = UUID.randomUUID().toString().replace("-", "");
							
							A : for(int i = 0 ; i< dataList.size() ; i++){
								
								Tuple2<String, LogE> tuple = getLogM(i , dataList , getPhone , sigletag);
								
								if("continue".equals(tuple._1)){
									continue A;
								}
								
								if(tuple._1!= null){
									getPhone = tuple._1;
								}
								resultList.add(tuple._2);
							}
						}
						//确保其电话号码一定有值,全部电话为空那也没办法了
						logE.setPhone(getPhone);
					}catch(Exception e){
						System.out.println(jsonData);
						System.out.println("小转化错误!!!!!");
					}
					
					break;
				case "againorder":
					//拆单
					String jsonArrayStr = map.get("cart_id").toString();
					try{
						ArrayList<?> pidJsonList = JSON.parseObject(jsonArrayStr, ArrayList.class);
						Set<String> set = new HashSet<String>();
						for (Object objStr : pidJsonList) {
							@SuppressWarnings("unchecked")
							HashMap<String , String > pidMap = JSON.parseObject(objStr.toString(), HashMap.class);
							//复制一个对象
							//pid可能存在重复数据,所以在此进行去重操作
							if(pidMap.get("pid")  != null){
								set.add(pidMap.get("pid").trim());
							}
						}
						if(set.size()== 0){
							LogE logE1  = (LogE) logE.clone();
							logE1.setType_id(null);
							logE1.setType("pid");
							resultList.add(logE1);
						}else{
							for (String pid : set) {
								LogE logE1  = (LogE) logE.clone();
								logE1.setType_id(pid);
								logE1.setType("pid");
								logE1.setProduct_id(pid);
								resultList.add(logE1);
							}
						}
						
					}catch(Exception e){
						System.out.println(jsonArrayStr);
						System.out.println("小转化错误!!!!!!!!!!!");
						
					}
					//直接返回
					return resultList;
				default:
					logE.setType_id(null);
					logE.setType(null);
					break;
			}
			
		}catch(Exception e ){
			System.out.println(jsonStr);
			System.out.println("大转化出现问题 !!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
			//json转化出错,直接全部设置为NULL
			logE.setType_id(null);
			logE.setType(null);
			logE.setTask(null);
			logE.setEshop_id(null);
			logE.setCreate_date(null);
			logE.setOrder_id(null);
		}
		
		resultList.add(logE);
		return resultList ;
	}
	
	
	/**
	  * Title: getLogM
	  * Description: 获取 logM对象(LogE的一种)
	  * @param dataList
	  * @return
	 */
	public static Tuple2<String, LogE> getLogM(int i , ArrayList<?> dataList , String getPhone , String singletag ){
		
		String jsonObjStr=  dataList.get(i).toString();
		@SuppressWarnings("unchecked")
		HashMap<String,String> dataMap  = JSON.parseObject(jsonObjStr , HashMap.class);
		Tuple2<String,LogE> errorTuple = new Tuple2<String,LogE>("continue" , null);
		
		String jt = dataMap.get("jt") == null ? null : dataMap.get("jt").toString();
		if(jt == null){
			return errorTuple;
		}else{
			LogE  logM = new LogE();
			logM.setSingle_tag(singletag);
			logM.setId((UUID.randomUUID().toString().replace("-", "")));
			logM.setBooth(dataMap.get("booth"));
			logM.setCell(dataMap.get("cell"));
			logM.setExh(dataMap.get("exh"));
			logM.setJc(dataMap.get("jc"));
			logM.setJt(jt);
			//对应的行为设置上商品id
			if("sku".equals(jt) || "groupon".equals(jt)){
				logM.setProduct_id(jt);
			}
			//设置E店id
			if("eshop".equals(jt)){
				logM.setEshop_id(jt);
			}
			logM.setCreate_date(dataMap.get("time"));
			logM.setLog_type("mongo_log");
			//电话,可能没有,没有的话就取上一个时间的电话
			String thisPhone = dataMap.get("phone");
			if(thisPhone != null){
				//不等于NUll的时候赋值给getPhone
				getPhone =  dataMap.get("phone").toString();
				//给此条logE记录设置phone
				logM.setPhone(thisPhone);
			}else{
				//上一个有的情况
				if(getPhone != null){
					logM.setPhone(getPhone);
				}else{
					//上一个没有的情况
					//从本条开始往下找一个不为空的
					//防止其越界
					if(i != (dataList.size() -1)){
						String nextPhone = null;
						N: for(int j = i+1 ; j < dataList.size() ; j++){
							@SuppressWarnings("unchecked")
							HashMap<String,String> nextMap  = JSON.parseObject( dataList.get(j).toString() , HashMap.class);
							nextPhone = nextMap.get("phone") == null ? null :nextMap.get("phone").toString();
							//有不为null的就退出循环
							if(nextPhone != null ){
								logM.setPhone(nextPhone);
								getPhone = nextPhone;
								break N;
							}
						}
					}
				}
			}
			Tuple2<String,LogE> tuple = new Tuple2<String,LogE>(getPhone,logM);
			return tuple ;
		}
	}
}

