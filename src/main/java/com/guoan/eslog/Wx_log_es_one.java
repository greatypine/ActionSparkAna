package com.guoan.eslog;

import static org.elasticsearch.spark.rdd.api.java.JavaEsSpark.esRDD;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.guoan.pojo.LogW_F;
import com.guoan.utils.FileUtil;

import net.sf.json.JSONException;
import net.sf.json.JSONObject;
import scala.Tuple2;

/**
  * Description:  行为日志清洗(es数据) (查询方式为拼接sql进行查询)
  * require : cid_phone 表
  * @author lyy  
  * @date 2018年6月5日
 */
public class Wx_log_es_one {
	//es索引库
	private static final String indexPath = "guoanshequ-*";
	private static final String tableName = "log_wx_m_c";
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
        conf.set("spark.reducer.maxSizeInFlight", "1024m");
        conf.set("spark.reducer.maxMblnFlight", "1024m");

        
		 //读取测试hive数据
		 SparkSession spark = SparkSession
			      .builder()
			      .config(conf)
			      .enableHiveSupport()
			      .appName("Spark Action_es_wx_one")
			      .getOrCreate();
		 
		 JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
		 jsc.setLogLevel("INFO");
		 
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
		 JavaRDD<LogW_F> resultRDD = filterRDD.mapPartitions(new FlatMapFunction<Iterator<String>, LogW_F>() {

			private static final long serialVersionUID = -3165202775071467559L;

			@Override
			public Iterator<LogW_F> call(Iterator<String> iterator) throws Exception {
				
				List<LogW_F> resultList = new ArrayList<LogW_F>();
				
				//大循环
				A : while(iterator.hasNext()){
					String line = iterator.next();
					
					//根据 "传递参数" 进行分割
					String[] arr1 = line.split("传递参数");
					if(arr1.length <= 1){
						continue A;
					}
					LogW_F logWF = new LogW_F();
					//Json转化
					try{
						
						JSONObject fromObject = JSONObject.fromObject(arr1[1].trim());
						logWF.setCustomer(fromObject.get("customer") == null || "".equals(fromObject.get("customer").toString().trim()) ? null : fromObject.get("customer").toString());
						logWF.setMdop(fromObject.get("mdop") == null  || "".equals(fromObject.get("mdop").toString().trim())  ? null : fromObject.get("mdop").toString());
						//加入返回结果集
						resultList.add(logWF);
					}catch(JSONException e){
						System.out.println(arr1[1]);
					}catch(Exception e){
						e.printStackTrace();
						System.out.println(arr1[1]);
					}
				}
				return resultList.iterator();
			}
		});
		 
		 //写入hive
		 Dataset<Row> createDataFrameRDD = spark.createDataFrame(resultRDD, LogW_F.class);
		 try {
			createDataFrameRDD.createTempView("log_tmp_wx_f");
		} catch (AnalysisException e) {
			System.out.println("log_tmp_wx_f 临时表创建失败");
			e.printStackTrace();
		}
		 
		 //有就删除
		spark.sql("drop table if exists gemini."+tableName+" purge ").count();
		 //写入hive
		spark.sql("create table gemini."+tableName+" as select * from log_tmp_wx_f").count();
		spark.close();
		jsc.close();	
		
	}
	
	
}

