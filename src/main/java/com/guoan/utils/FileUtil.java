package com.guoan.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
  * Description: 文件读写工具类
  * @author lyy  
  * @date 2018年5月4日
 */
public class FileUtil {

	
	private static  Properties properties = null;
	
	
	/**
	  * Title: readLine
	  * Description: 从指定路径读取文件
	  * @param path
	  * @return
	 */
	public static List<String> readLine(String path){
		
		List<String> list = new ArrayList<String>();
		
		FileReader fRead = null;
		BufferedReader read  = null;
		try {
			fRead = new FileReader(path);
			read = new BufferedReader(fRead);
			String str = "";
			while((str = read.readLine()) != null){
				list.add(str.trim());
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			try {
				read.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	      
		return list;
	}
	
	/**
	  * Title: write
	  * Description: 写内容到指定路径
	  * @param list
	  * @param path
	 */
	public static void write(List<String> list,String path){
		
		FileWriter w = null;
		BufferedWriter writer =null;
		try {
			w = new FileWriter(path,true);
			writer = new BufferedWriter(w);
			for (String str : list) {
				writer.write(str);
				writer.flush();
				writer.newLine();
			}
			System.out.println("ok");
		} catch (Exception e1) {
			e1.printStackTrace();
		}finally{
			try {
				w.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	      
	}
	
	/**
	  * Title: getProperties
	  * Description: 读取配置文件根据key获得value
	  * @param key
	  * @return
	 */
	public static String getProperties(String key ,String path){
		
		if(properties == null){
			properties = new Properties();
		}
		InputStream in = FileUtil.class.getClassLoader().getResourceAsStream(path);
		try {
			properties.load(in);
		} catch (IOException e) {
			System.out.println("配置文件读取异常!!!!!!!!!");
			e.printStackTrace();
		}
		
		String value = properties.getProperty(key);
		
		return value.trim();
	}
	
	
	
	
}
