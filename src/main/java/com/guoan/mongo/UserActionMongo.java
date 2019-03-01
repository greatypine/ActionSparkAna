package com.guoan.mongo;

import java.net.URLEncoder;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.guoan.pojo.UserActionLog;
import com.guoan.utils.FileUtil;
import com.guoan.utils.ImpalaQueryPoolUtils;
import com.guoan.utils.UserActionUtils;
import com.mongodb.spark.MongoSpark;

import scala.Tuple2;

/**
  * Description:  用户行为分析(mongodb数据)   
  * @author lyy  
  * @date 2018年5月02日
 */
public class UserActionMongo {
	
	private static String mongoIp = null ;
	private static String mongoPassword = null ;
	private static String mongoPort = null ;
	private static String mongoDatabase = null ;
	private static String mongoTable = null ;
	private static String sparkLogLevel = null;
	private static final String configPath = "conf/config.properties";

	public static void main(String[] args)  throws Exception {
		
		 mongoIp = FileUtil.getProperties("mongoIp",configPath);
		 mongoPassword = FileUtil.getProperties("mongoPassword",configPath);
		 mongoPort = FileUtil.getProperties("mongoPort",configPath);
		 mongoDatabase = FileUtil.getProperties("mongoDatabase",configPath);
		 mongoTable = FileUtil.getProperties("mongoTable",configPath);
		 sparkLogLevel = FileUtil.getProperties("sparkLogLevel",configPath);
		
		//密码中有特殊字符,需要url编码
		String url = "mongodb://"+mongoDatabase+":"+URLEncoder.encode(mongoPassword,"UTF-8")+"@"+mongoIp+":"+mongoPort+"/"+mongoDatabase+"."+mongoTable;
			
		SparkSession spark = SparkSession.builder()
                .appName("UserActionMongo")
                .config("spark.mongodb.input.uri", url)
                .config("spark.mongodb.output.uri", url)
                .getOrCreate();
		 // 创建jsc 对象
	    JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

	    jsc.setLogLevel(sparkLogLevel);
		//创建spark 累加器
//		final LongAccumulator rowNum = spark.sparkContext().longAccumulator("rowN");
		//广播变量
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		final Broadcast<SimpleDateFormat> sdfBroadcast = jsc.broadcast(sdf);
	    
	    // Load data with explicit schema
	    Dataset<UserActionLog> explicitDS = MongoSpark.load(jsc).toDS(UserActionLog.class).repartition(1);
		//注册为临时表
	    explicitDS.createOrReplaceTempView("user_action");
	    
		String sql = "SELECT exh,jc ,jt, phone,time ,type   "
				+ "FROM (SELECT ROW_NUMBER() OVER(PARTITION BY phone  ORDER BY time  ) rn, "
				+ "user_action.*  "
				+ "FROM user_action  where phone is not null  "
				+ "and phone = '15840045530'  ) "; 
//				+ "and substring(time,0,10) = '2018-04-25'   )     ";
		
		//spark sql,不能分区,分区顺序容易乱
		Dataset<Row> dataset2 = spark.sql(sql).repartition(1);
		
		//<phone,row>
		JavaPairRDD<String, Row> phoneRowRDD = dataset2.javaRDD().flatMapToPair(new PairFlatMapFunction<Row, String, Row>() {
			private static final long serialVersionUID = 119198219882198L;

			@Override
			public Iterator<Tuple2<String, Row>> call(Row row) throws Exception {
				ArrayList<Tuple2<String, Row>> list = new ArrayList<Tuple2<String,Row>>();
				
				list.add(new Tuple2<String,Row>(row.getAs("phone")+"",row));
				
				return list.iterator();
			}
		});
		//缓存
		JavaPairRDD<String, Row> phoneRowCacheRDD = phoneRowRDD.cache();
		//增加分区
		phoneRowCacheRDD.repartition(1);
		
		//<phone,list<row>>  >> 着个分析 
		JavaPairRDD<String, List<Object[]>> userActionPathRdd = phoneRowCacheRDD.groupByKey().mapToPair(new PairFunction<Tuple2<String,Iterable<Row>>, String, List<Object[]>>() {
			private static final long serialVersionUID = 1219882219882L;

			@Override
			public Tuple2<String, List<Object[]>> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {
				//调用产生用户行为轨迹的方法
				List<Object[]> userActionPathList = UserActionUtils.getUserActionPath(tuple._2);
				
				return new Tuple2<String,List<Object[]>>(tuple._1,userActionPathList);
			}
		});
		//缓存下来
		JavaPairRDD<String, List<Object[]>> userActionPathRddCache = userActionPathRdd.cache();
		
		//着条轨迹进行比对,分析其是否存在购买行为
		//<深度_是否购买,1>
		JavaPairRDD<String, Integer> depthRDD = userActionPathRddCache.flatMapToPair(new PairFlatMapFunction<Tuple2<String,List<Object[]>>, String, Integer>() {

			private static final long serialVersionUID = 11219812198988L;

			@Override
			public Iterator<Tuple2<String, Integer>> call(Tuple2<String, List<Object[]>> tuple) throws Exception {
				
				//判断用户是否购买
				List<Tuple2<String, Integer>> returnList = judgeUserBuyAction(sdfBroadcast , tuple);
				
				return returnList.iterator();
			}
		});
		
		//进行统计
		depthRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
			private static final long serialVersionUID = 121912198128121L;

			@Override
			public Integer call(Integer arg0, Integer arg1) throws Exception {
				return arg0+arg1;
			}
		}).foreach(new VoidFunction<Tuple2<String,Integer>>() {
			private static final long serialVersionUID = 121981121981221L;

			@Override
			public void call(Tuple2<String, Integer> tuple) throws Exception {
				System.out.println(tuple._1+"  ==  "+tuple._2);
			}
		});
		
		System.out.println("== system.end ==");
		
		//关闭资源
		spark.close();
		jsc.close();
	}
	
	
	//判断用户是否发生有效购买
	public static List<Tuple2<String, Integer>> judgeUserBuyAction(Broadcast<SimpleDateFormat> sdfBroadcast , Tuple2<String, List<Object[]>>  tuple) throws Exception{
		
		SimpleDateFormat sdf = sdfBroadcast.value();
		
		List<Tuple2<String, Integer>> returnList = new ArrayList<Tuple2<String, Integer>>();
		
		//判断时间间隔是否大于10s
		List<Object[]> list = tuple._2;
		
		Row row1 = null;
		Row row2 = null;
		String date1Str = "";
		String date2Str = "";
		
		System.out.println("list size = " + list.size());
		
		if(list!= null && list.size()>0){
			
			//为方便比对,后面加一条
			list.add(list.get(0));
			
			//两两对比
			A : for(int i = 0 ; i < list.size()-1 ; i++){
				
				System.out.println("比对深度="+i);
				
				//是否发生有效购买的标记
				boolean buyFlag = false;
				
				Object[] obj1 = list.get(i);
				Object[] obj2 = list.get(i+1);
				//判空,各自取行为轨迹的最后一条
				if(obj1!= null && obj1.length>0){
					row1 =  (Row)obj1[obj1.length-1];
				}
				if(obj2!= null && obj2.length>0){
					row2 =  (Row)obj2[obj2.length-1];
				}
				//获取每条记录的时间
				if(row1 != null && row2 != null){
					date1Str = row1.getAs("time");
					
					if(i == list.size()-2){
						//最后一条为自己添加,所以时间范围选择往后推最广的范围
						date2Str = "2999-10-10 10:10:10";
					}else{
						date2Str = row2.getAs("time");
					}
					
				}else{
					//有空的情况
					//??????????????????????????????????????
				}
				
				String sql = "";
				
				if(!"".equals(date1Str) && !"".equals(date2Str)  ){
					
					//判断两个时间差超没超过10000,并且不是最后一次比对
					if(sdf.parse(date2Str).getTime() - sdf.parse(date1Str).getTime() <= 10000 && i != list.size()-2){
						//继续下个循环
						//记录深度
						returnList.add(new Tuple2<String,Integer>((obj1.length+1)+"_No",1));
						
						continue A;
					}else{
						
						//==================================
						System.out.println(tuple._1+" === isBuy " );
						//判断是否发生购买行为
						sql= " SELECT toi.eshop_pro_id  AS eshopProId , toi.order_id AS orderId ,toi.create_user_id AS createUserId  "
								+ " FROM t_order_item toi  LEFT JOIN t_customer tc ON toi.create_user_id = tc.id	"
								+ " WHERE tc.mobilephone = '"+tuple._1+"'  "
								+ " AND toi.create_time BETWEEN '"+date1Str+"' AND '"+date2Str+"' ";
						
						System.out.println("isbuysql = "+sql);
						
						List<Map<String, Object>> userBuyList = ImpalaQueryPoolUtils.getList(sql);
						
						if(userBuyList != null && userBuyList.size()>0){
							
							System.out.println("is buy list size = " + userBuyList.size());
							
							String eshopProId = null;
							String orderId = null;
							
							//说明用户有下单购买行为
							//判断 row1 的 jt 是什么类型,区别分析
							if("group".equals(row1.getAs("jt").toString().trim())){
								
								System.out.println("is group ");
								
								
								String contentTag = row1.getAs("jt").toString().trim();
								//可能中间有好多条,需要着条去比对
								B1 : for(Map<String,Object> userBuyMap : userBuyList){
									
									eshopProId = userBuyMap.get("eshopProId")+"";
									//判空
									if(eshopProId != null && !"".equals(eshopProId) && !"null".equals(eshopProId) && contentTag != null && !"".equals(contentTag) ){
										
										//==================================
										System.out.println(tuple._1+" === groupisBuy " );
										
										sql = " select tp.id FROM t_order_item toi "
												+ "  join t_product tp on tp.id = toi.eshop_pro_id "
												+ "  and tp.content_tag like '%"+contentTag.trim()+"%' "
												+ "  and toi.eshop_pro_id = '"+eshopProId+"' ";
										
										System.out.println("groupisBuy sql = "+sql);
										
										List<Map<String, Object>> groupList = ImpalaQueryPoolUtils.getList(sql);
										
										if(groupList != null && groupList.size()>0 ){
											
											System.out.println("group size = "+groupList.size());
											
											//说明发生了有效购买 (group)
											buyFlag = true;
											//跳出循环
											break B1;
										}
									}
								}
							}else if("sku".equals(row1.getAs("jt").toString().trim())){
								
								System.out.println("si sku ");
								
								String phoneNum = tuple._1;
								String skuId = row1.getAs("jc").toString().trim();
								
								//判空
								if(skuId != null && !"".equals(skuId)){
									
									//==================================
									System.out.println(tuple._1+" === skuisBuy " );
									
									sql ="SELECT id  "
											+ "  FROM t_order_item   where eshop_pro_id = '"+skuId.trim()+"' "
											+ "  and create_user ='"+phoneNum+"'  "
											+ "  and create_time between '"+date1Str+"' and  '"+date2Str+"' ";
									
									
									System.out.println("skuisBuy sql = "+sql);
									
									List<Map<String, Object>> skuList = ImpalaQueryPoolUtils.getList(sql);
									if(skuList != null && skuList.size()>0 ){
										//说明发生了有效购买 (sku)
										buyFlag = true;
									}
								}
							}else if ("groupon".equals(row1.getAs("jt").toString().trim())){
								
								System.out.println("si groupon");
								
								//可能中间有好多条,需要着条去比对
								B3 : for(Map<String,Object> userBuyMap : userBuyList){
									
									orderId = userBuyMap.get("orderId")+"";
									
									if(orderId != null && !"null".equals(orderId) && !"".equals(orderId)){
										
										//==================================
										System.out.println(tuple._1+" === grouponisBuy " );

										sql = "SELECT groupon_instance_id FROM t_order "
												+ "where id= '"+orderId.trim()+"'";
										List<Map<String, Object>> grouponList = ImpalaQueryPoolUtils.getList(sql);
										//发生有效购买行为
										if(grouponList!= null && grouponList.size() >0){
											
											System.out.println("groupon size = " + grouponList.size() );
											buyFlag = true;
											break B3;
										}
									}
								}
								
							}else if("eshop".equals(row1.getAs("jt").toString().trim())){
								
							System.out.println(" is eshop ");
								
								String eshopId = row1.getAs("jc").toString().trim();
								
								if(eshopId != null && !"".equals(eshopId)){
									B4: for(Map<String,Object> userBuyMap : userBuyList){
										eshopProId = userBuyMap.get("eshopProId")+"";
										//判空
										if(eshopProId != null && !"null".equals(eshopProId) && !"".equals(eshopProId)){
											
											//===============================================
											System.out.println(tuple._1+" === eshopisBuy " );
											
											sql = "SELECT id  FROM t_product  "
													+ "  WHERE eshop_id = '"+eshopId.trim()+"' "
													+ "	AND id = '"+eshopProId.trim()+"'";
											
											System.out.println("eshopisBuy sql = "+sql);
											
											List<Map<String, Object>> eshopList = ImpalaQueryPoolUtils.getList(sql);
											
											if(eshopList != null && eshopList.size() >0){
												
												System.out.println("eshop size = " + eshopList.size());
												
												//发生有效购买行为(eshop)
												buyFlag = true;
												break B4;
											}
										}
									}
								}
							}else{
								//发生的购买行为不属于这个轨迹
							}
						}else{
							//用户没有购买行为
						}
					}
				}
			//封装返回值
			if(buyFlag){
				returnList.add(new Tuple2<String,Integer>((obj1.length+1)+"_Yes",1));
			}else{
				
				returnList.add(new Tuple2<String,Integer>((obj1.length+1)+"_No",1));
			}
			}//end A
		}
		
		return returnList;
	}
	
}
