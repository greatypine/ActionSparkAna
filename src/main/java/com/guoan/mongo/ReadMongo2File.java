package com.guoan.mongo;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.bson.Document;

import com.guoan.utils.FileUtil;
import com.mongodb.BasicDBObject;
import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;


/**
  * Description:  根据条件过滤mongo数据并保存到本地
  * @author lyy  
  * @date 2018年6月13日
 */
public class ReadMongo2File {
	

	private static String mongoIp = null ;
	private static String userName = "gemini" ;
	private static String mongoPassword = null ;
	private static String mongoPort = null ;
	private static String mongoDatabase = null ;
	private static final String configPath = "conf/config.properties";
	
	/**
	 * 
	  * Title: rm2f
	  * Description: 调用方法 
	  * @param path
	  * @param condition
	  * @param param
	 */
	public static  void rm2f(String path  , String condition , String param ){
		
		
		File wFile=  new File(path);
		if(wFile.isDirectory()){
			return ;
		}else{
			if(wFile.exists()){
				wFile.deleteOnExit();
			}
			List<String> collection = getCollection("api_log",condition, param);
			
			System.out.println(collection.size());
			
			//写入本地
			FileUtil.write(collection, path);
		}
	}
	
	/**
	  * Title: getCollection
	  * Description: 查询数据
	  * @param collectionName
	  * @param condition
	  * @param param
	  * @return
	 */
	private static List<String>  getCollection(String collectionName , String condition , String param  ){
		
		List<String>  wList=  new ArrayList<String>();
		
		 mongoIp = FileUtil.getProperties("mongoIp",configPath);
		 mongoPassword = FileUtil.getProperties("mongoPassword",configPath);
		 mongoPort = FileUtil.getProperties("mongoPort",configPath);
		 mongoDatabase = FileUtil.getProperties("mongoDatabase",configPath);
		
		 //ServerAddress()两个参数分别为 服务器地址 和 端口  
        ServerAddress serverAddress = new ServerAddress(mongoIp,Integer.valueOf(mongoPort));  
        List<ServerAddress> addrs = new ArrayList<ServerAddress>();  
        addrs.add(serverAddress);  
          
        //MongoCredential.createScramSha1Credential()三个参数分别为 用户名 数据库名称 密码  
        MongoCredential credential = MongoCredential.createScramSha1Credential(userName, mongoDatabase, mongoPassword.toCharArray());  
        List<MongoCredential> credentials = new ArrayList<MongoCredential>();  
        credentials.add(credential);  
          
        //通过连接认证获取MongoDB连接  
        MongoClient mongoClient = new MongoClient(addrs, credentials);  
          
        //连接到数据库  
        MongoDatabase mongoGetDatabase = mongoClient.getDatabase(mongoDatabase);  
        System.out.println("Connect to database successfully");  
        
        MongoCollection<Document> collection = mongoGetDatabase.getCollection(collectionName);
        System.out.println("集合 "+collectionName+"选择成功");
        
        FindIterable<Document> getList= collection.find(new BasicDBObject(new Document(condition,param)));
		getList.forEach(new Block<Document>() {
			@Override
			public void apply(Document doc) {
				
				//有requestUri的就留下
				if(doc.get("requestUri") != null){
					wList.add(doc.toJson());
				}
			}
		});
        
		mongoClient.close();
		return wList;
		
	}
	
	
	public static void main(String[] args) {
		
		ReadMongo2File.rm2f("C:\\Users\\Administrator\\Desktop\\api.log"  , "createDate", "2018-07-13");
	}
	
}
