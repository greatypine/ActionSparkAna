package com.guoan.eslog.savefile;

import com.guoan.mongo.ReadMongo2File;
import com.guoan.utils.HDFSUtils;

public class Api_log_saveTextfile {

	private static final String savePath = "/root/log/api/";
	
	/**
	  * Title: saveApiFile2Hdfs
	  * Description: 读取api数据并上传到hdfs种
	  * @param dateStr 日期  yyyy-MM-hh
	  * @throws Exception
	 */
	public static void saveApiFile2Hdfs( String dateStr) throws Exception{
		String path = savePath+dateStr.replace("-", "")+".log";
		 //获取数据
		ReadMongo2File.rm2f(path  , "createDate", dateStr);
		//上传到hdfs
		String saveHdfsPath = "/user/log/api/";
		String apiPath = savePath+dateStr.replace("-", "")+".log";
		HDFSUtils.uploadFile(apiPath, saveHdfsPath+dateStr.replace("-", "")+".log");
	}
	
}
