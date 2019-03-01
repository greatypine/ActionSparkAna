package com.guoan.hive;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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

import com.guoan.pojo.ActivityResult;
import com.guoan.utils.ImpalaPoolUtils;

import scala.Tuple2;
/**
  * Description:   活动分析,hive数据(已经上线)
  * @author lyy  
  * @date 2018年5月22日
 */
public class Activity_Hive {
	
	
	public static void main(String[] args) {
		//三个月的毫秒数
		long threeMonthMill =  7776000000L;
		//分区数量
		int partitionNum = 6; 
		//表名
		String  tableName = "activity_analyze";

		//校验参数
		Map<String, String> paramCheck = paramCheck(args);
		
		String  conditionStr = paramCheck.get("conditionStr");
		final String  dateType = paramCheck.get("dateType");
		final String  analyze = paramCheck.get("analyze");
		
		 //读取测试hive数据
		 SparkSession spark = SparkSession
			      .builder()
//			      .master("local")
			      .appName("Spark Activity_Hive1")
			      .enableHiveSupport()
			      .getOrCreate();
		 
		 JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
		 jsc.setLogLevel("INFO");
		 SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		 final Broadcast<SimpleDateFormat> broadcastSdf = jsc.broadcast(sdf);
		 SimpleDateFormat simpleSdf = new SimpleDateFormat("yyyy-MM-dd");
		 final Broadcast<SimpleDateFormat> broadcastSimpleSdf = jsc.broadcast(simpleSdf);
		 
		 //创建impala工具类的广播变量
		 final ImpalaPoolUtils impalaUtil = new ImpalaPoolUtils();
		 
		 String sql = "select * from gemini.activity_dataset  where  1=1  "
		 		+ "    and  from_unixtime(unix_timestamp(ordercreatetime), 'yyyy-MM-dd') "
		 		+conditionStr;
		 
		 Dataset<Row> dataset = spark.sql(sql);
		 
 		//进行分区
		 JavaRDD<Row> rowRDD = dataset.javaRDD().repartition(partitionNum);
		 
		 //map进行格式化< 活动1_活动2_xxx , row> 
		 JavaPairRDD<String, Row> mapRDD = rowRDD.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Row>, String, Row>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<Tuple2<String, Row>> call(Iterator<Row> iterator) throws Exception {
				
				//创建结果集
				List<Tuple2<String, Row>> list = new ArrayList<Tuple2<String, Row>>();
				
				while(iterator.hasNext()){
					
					Row row = iterator.next();
					String key = "";
					//顺序是 优惠券,团购,sku,省id,市id,区id,事业群id,频道id
					
					String type_id = row.getAs("type_id") == null ? null : row.getAs("type_id").toString() ;
					key += type_id +"_";
					
					String groupon_instance_id = row.getAs("groupon_instance_id") == null ? null : row.getAs("groupon_instance_id").toString() ;
					key += groupon_instance_id +"_";
					
					String sku_rule_id = row.getAs("sku_rule_id") == null ? null : row.getAs("sku_rule_id").toString() ;
					key += sku_rule_id +"_";
					
					String province_code = row.getAs("province_code") == null ? null : row.getAs("province_code").toString() ;
					key += province_code +"_";
					
					String city_code = row.getAs("city_code") == null ? null : row.getAs("city_code").toString() ;
					key += city_code +"_";
					
					String ad_code = row.getAs("ad_code") == null ? null : row.getAs("ad_code").toString() ;
					key += ad_code+"_";
					
					String bussiness_group_id = row.getAs("bussiness_group_id") == null ? null : row.getAs("bussiness_group_id").toString() ;
					key += bussiness_group_id+"_";
					
					String channel_id = row.getAs("channel_id") == null ? null : row.getAs("channel_id").toString() ;
					key += channel_id;
					
					list.add(new Tuple2<String, Row>(key, row));
				}
				return list.iterator();
			}
		});
		 
		 //重新分区,相同的key分到同一分区
		 JavaPairRDD<String, Row> partitionByRDD = mapRDD.partitionBy(new Partitioner() {
			 
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
		});

		 //缓存上步结果
		 JavaPairRDD<String, Row> partitionByCacheRDD = partitionByRDD.cache();
		 
		 // 封装成用 , 分割的字符串,方便后期可以存到hive中
		 //<key, rows>  === > string....
		 JavaPairRDD<String, Iterable<Row>> groupByKeyRDD = partitionByCacheRDD.groupByKey();
		
		//保留父RDD的所有信息,分区进行map操作,提升效率
		JavaRDD<ActivityResult> resultRDD = groupByKeyRDD.mapPartitions(new FlatMapFunction<Iterator<Tuple2<String,Iterable<Row>>>, ActivityResult>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<ActivityResult> call(Iterator<Tuple2<String, Iterable<Row>>> iter) throws Exception {
				//返回结果集
				List<ActivityResult> resultList= new ArrayList<ActivityResult>();
				SimpleDateFormat sdf = broadcastSdf.value();
				SimpleDateFormat simpleSdf = broadcastSimpleSdf.value();
				
				while(iter.hasNext()){
					
					Tuple2<String, Iterable<Row>> tuple = iter.next();
					
					//用于排序的treeMap
					TreeMap<Long,Row> treeMap = new TreeMap<Long,Row>();
					
					Iterator<Row> iterator = tuple._2.iterator();
					//排序得到时间区间
					while(iterator.hasNext()){
						Row row = iterator.next();
						//获取时间进行转换,因为t_order和t_customer表中创建时间字段都不为空,所以不用判空
						long timeAsLong = sdf.parse(row.getAs("ordercreatetime").toString()).getTime();
						treeMap.put(timeAsLong, row);
					}
					
					//treeMap 中已经从大到小排好序列
					//创建一个Tuple ,存储时间区间
					Tuple2<Long,Long> timeTuple = new Tuple2<Long, Long>(treeMap.firstKey(), treeMap.lastKey()); 
					
					//遍历map,获得需要的三个结果
					
					int newUser = 0; //拉新
					int returnUser = 0 ;//回流
					long saleNum = 0; //总销量
					double saleMoneg = 0.0;//总销售额
					
					
					String sql = "";
					
					List<String> newUserIdList = new ArrayList<String>();
					List<String> returnUserIdList = new ArrayList<String>();
					
					for(Long  mapKey :  treeMap.keySet()){
						
						Row row = treeMap.get(mapKey);
						
						long userCreateTimeAsLong = sdf.parse(row.getAs("usercreatetime").toString()).getTime();
						//下单时间,前一秒
						String endTime = sdf.format(new Date(mapKey - 1000));
						
						//判断注册时间是否在活动区间范围内,在就有可能属于拉新
						if(userCreateTimeAsLong >= timeTuple._1 && userCreateTimeAsLong <= timeTuple._2){
							
							//判断是否这个用户已经先前查询过,有查询过的话就不用再次查询了
							if(!newUserIdList.contains(row.getAs("userid")+"")){
								//如果在这个区间内 ,判断之前是否有订单
								//用户创建时间
								String startTime = sdf.format(new Date(userCreateTimeAsLong));
								
								sql =" select  id  from t_order where  1=1  "
										+ " and  customer_id  = '"+row.getAs("userid")+"' "
										+ " and  from_unixtime(unix_timestamp(create_time ), 'yyyy-MM-dd HH:mm:ss') "
										+ "  between '"+startTime+"' and '"+endTime+"'  limit 1" ;
								
								//防止查询出现异常停止整个服务
								try{
									List<Map<String, Object>> newUserList = impalaUtil.getList(sql);
									
									if(newUserList == null || newUserList.size()  == 0){
										//只有没查到的情况才属于拉新
										newUser ++;
									}
								}catch(Exception e){
									System.out.println("newUser查询出现异常   "+sql);
									e.printStackTrace();
								}
								
								newUserIdList.add(row.getAs("userid")+"");
							}
							
						}else{
							//判断是否这个用户已经先前查询过,有查询过的话就不用再次查询了
							
							if(!returnUserIdList.contains(row.getAs("userid")+"")){
								//判断是否是回流用户,看其三个月内是否有订单
								String startTime = sdf.format(new Date(mapKey - threeMonthMill));
								sql = "select id from t_order where 1=1  "
										+ " and  customer_id  = '"+row.getAs("userid")+"'   "
										+ " and  from_unixtime(unix_timestamp(create_time ), 'yyyy-MM-dd HH:mm:ss') "
										+ " between '"+startTime+"' and '"+endTime+"'  limit 1" ;
								
								
								//防止查询出现异常停止整个服务
								try{
									List<Map<String, Object>> returnUserList = impalaUtil.getList(sql);
									
									if(returnUserList == null || returnUserList.size() == 0){
										//只有三个月内没有下单的用户才属于回流用户
										returnUser ++;
									}
								}catch(Exception e){
									System.out.println("returnUser查询出现异常   "+sql);
									e.printStackTrace();
								}
								
								returnUserIdList.add(row.getAs("userid")+"");
							}
						}
						//销售量增加
						Integer num = Integer.valueOf(row.getAs("quantity").toString());
						saleNum+= num;
						//销售额增加
						//判断是否拆单
						boolean isSplit = row.getAs("is_split").toString().equals("yes") ? true : false ; 
						
						if(isSplit){
							//原价乘以数量
							double originalPrice = row.getAs("original_price") == null ? 0.0 : Double.valueOf(row.getAs("original_price").toString());
							saleMoneg += (originalPrice * num);
							
						}else{
							//单价乘以数量
							double originalPrice = row.getAs("unit_price") == null ? 0.0 : Double.valueOf(row.getAs("unit_price").toString());
							saleMoneg += (originalPrice * num);
						}
						
					} //end treeMap.keySet()
					
					//封装返回的结果对象
					//封装维度id
					String[] ids = tuple._1.split("_");
					ActivityResult result = new ActivityResult();
					//uuid
					result.setId(UUID.randomUUID().toString().replace("-", ""));
					//顺序是 优惠券,团购,sku,省id,市id,区id,事业群id,频道id
					result.setType_id(ids[0]);
					result.setGroupon_instance_id(ids[1]);
					result.setSku_rule_id(ids[2]);
					result.setProvince_code(ids[3]);
					result.setCity_code(ids[4]);
					result.setAd_code(ids[5]);
					result.setBussiness_group_id(ids[6]);
					result.setChannel_id(ids[7]);
					//封装分析结果
					result.setNewUser(newUser);
					result.setReturnUser(returnUser);
					result.setSaleNum(saleNum);
					
					//销售额保留两位小数
					double round = (double)Math.round(saleMoneg*100)/100;
					result.setSaleMoneg(round);
					//优惠券,团购,sku,省id,市id,区id,事业群id,频道id,拉新,回流,总销量,总销售额
					
					if("one".equals(analyze)){
						//分析昨天
						
						//真实时间
						result.setSimple_date(dateType);
						//时间类型
						result.setDate_type("yesterday");
					}else{
						//分析一段时间
						
						//此次分析是分析一段时间活动的效果,订单属于整个活动周期,所以设置时间为(开始时间-结束时间) yyyy-mm-dd格式
						result.setSimple_date(simpleSdf.format(new Date(timeTuple._1))+"-"+simpleSdf.format(new Date(timeTuple._2)));
						//输入的时间类型
						result.setDate_type(dateType);
					}
					
					resultList.add(result);
				}
				
				return resultList.iterator();
			}
		}, true);
		
		 //写入hive
		 Dataset<Row> createDataFrameRDD = spark.createDataFrame(resultRDD, ActivityResult.class);
		 try {
			createDataFrameRDD.createTempView("activity_result_tmp");
			} catch (AnalysisException e) {
				System.out.println("activity_result_tmp 临时表创建失败");
				e.printStackTrace();
			}
		 
		 //以后保证表是存在的!!!!!!!!
		 
		 //spark临时表写入hive
//		 spark.sql("create table gemini."+tableName+" as select * from activity_result_tmp").count();
		 //分析结果存入表
		 spark.sql("insert into table gemini."+tableName+" select * from activity_result_tmp").count();
		 
		 jsc.close();
		 spark.close();
	}
	
	/**
	  * Title: paramCheck
	  * Description: 输入参数校验封装方法 
	  * @return
	 */
	public static Map<String,String>  paramCheck(String[] args){
		SimpleDateFormat simpleSdf = new SimpleDateFormat("yyyy-MM-dd");
		Map<String,String> resultMap = new HashMap<String,String>();
		/*
		 * --当没有传入参数的时候就是分析昨天的数据
		 * 
		 * --传入参数时
		 * args[0] 开始时间
		 * args[1] 结束
		 */
		String aStartTime = "";
		String aEndTime = "";
		String conditionStr = "";
		String dateType = "";
		
		//分析类型 ,one 为昨天, two 为时间段
		String analyze = "one"; 
		
		 if(args.length == 0 ){
			 dateType =  simpleSdf.format(  new Date(new Date().getTime()-24*60*60*1000));
			 aStartTime = dateType;
			 //拼接条件
			 conditionStr  =  "  =  '"+aStartTime+"' ";
		 }else if(args.length ==2){
			 aStartTime = args[0];
			 aEndTime = args[1];
			//拼接条件
			 conditionStr = "  between '"+aStartTime+"'  and  '"+aEndTime+"' ";
			 dateType = aStartTime+"-"+aEndTime;
			 analyze = "two";
		 }else{
			 throw new RuntimeException("参数输入错误 !!");
		 }
		
		resultMap.put("conditionStr", conditionStr);
		resultMap.put("dateType", dateType);
		resultMap.put("analyze", analyze);
		
		
		return resultMap;
	};
	
	
	
	
}
