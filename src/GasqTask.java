package com.gasq.bdp.task;

import org.apache.hadoop.conf.Configuration;

public interface GasqTask {
	public final static String DFS_MASTER_PATH = "hdfs://master:8020/";
	
	/**
	 * 获取默认hdfs配置
	 * @return
	 */
	default Configuration getConf() {
		Configuration hdfsConf = new Configuration();
		hdfsConf.set("fs.defaultFS", DFS_MASTER_PATH);
		return hdfsConf;
	}
}
