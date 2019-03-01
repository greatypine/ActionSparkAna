package com.guoan.utils;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

/**
  * Description:   hdfs 的工具类
  * @author lyy  
  * @date 2018年7月27日
 */
public class HDFSUtils {

//	private static final String nodePath ="hdfs://hadoop1:8020";
//	private static final String nodePath ="hdfs://node20013:8020";
	private static final String nodePath ="hdfs://node401:8020";
	
	static Configuration  conf = null;
	static FileSystem  fs = null;
	
	static{
		conf = new Configuration(false);
		conf.set("fs.defaultFS", nodePath);
		try {
			fs = FileSystem.get(conf);
		} catch (IOException e) {
			System.out.println("创建hdfs上下文对象出错!!!!");
			e.printStackTrace();
		}
	}

	
	/**
	  * Title: mkdir
	  * Description: 创建目录
	  * @param path
	 * @throws IOException 
	 */
	public static boolean mkdir(String path ) throws IOException{
		
		boolean targ = true;
		
		Path dir = new Path(path);
		//判断路径是否存在
		if(!fs.exists(dir)){
			targ = fs.mkdirs(dir);
		}
		return targ;
	}
	
	/**
	 * 
	  * Title: uploadFile 
	  * Description: 上传数据到hdfs
	  * @param inputPath 本地文件路径
	  * @param outputPath 写到hdfs的路径(包含要保存到的文件名)
	  * @return
	 */
	public static void uploadFile(String inputPath, String outputPath) throws Exception{
		//hdfs 写入path
		Path file = new Path(outputPath);
		FSDataOutputStream wPath = fs.create(file);
		//本地文件存储路径
		InputStream rPath = new BufferedInputStream(new FileInputStream(new File(inputPath)) ) ;
		//传输文件
		IOUtils.copyBytes(rPath, wPath, conf, true);
		System.out.println("写入hdfs完成...");
	}
	
	
	/**
	 * 
	  * Title: downloadFile
	  * Description: 将hdfs文件下载到本地
	  * @param inputPath hdfs文件路径
	  * @param outputPath 写到本地的文件路径
	 * @throws IOException 
	 */
	public static void downloadFile(String inputPath, String outputPath) throws IOException{
		
		//hdfs 写入path
		Path file = new Path(inputPath);
		FSDataInputStream rPath = fs.open(file);
		//本地文件存储路径
		BufferedOutputStream wPath = new BufferedOutputStream(new FileOutputStream(new File(outputPath)) ) ;
		//传输文件
		IOUtils.copyBytes(rPath, wPath, conf, true);
		System.out.println("下载完成...");
	}
	
	
	
	/**
	  * Title: close
	  * Description: 关闭资源
	 */
	public static void  close(){
		try {
			fs.close();
		} catch (IOException e) {
			System.out.println("关闭资源出错!!!!");
			e.printStackTrace();
		}
	}
	
	
	
	public static void main(String[] args) throws Exception {

//		downloadFile("/lyy/aaa.txt", "C:\\Users\\Administrator\\Desktop\\aaaa.txt");
//		uploadFile("C:\\Users\\Administrator\\Desktop\\HttpClientUtils.class", "/lyy2/aaa.txt");
		
		mkdir("/user/log/api");
		mkdir("/user/log/wx");
		mkdir("/user/log/eshop");
		
		close();
		
	}
	
	
	
}
