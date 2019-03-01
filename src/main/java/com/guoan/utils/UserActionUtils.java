package com.guoan.utils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.sql.Row;

/**
  * Description:  用户行为工具类 
  * @author lyy  
  * @date 2018年5月4日
 */
public class UserActionUtils {

	/**
	 * 
	  * Title: getUserActionPath
	  * Description:  获取用户行为轨迹
	  * @param interable
	  * @return
	 */
	public static List<Object[]> getUserActionPath(Iterable<Row> interable){

		
		//存放中间的行为轨迹
		ArrayList<Row> tempList = new ArrayList<Row>();
		//存放最后结果的行为估计
		List<Object[]> resultList = new  ArrayList<Object[]>();
		
		//存放上一步的jt值
		String lastJc = "";
		//着行分析轨迹
		Iterator<Row> iterator = interable.iterator();
		
		while(iterator.hasNext()){
			
			Row row = iterator.next();
			
			//拿到数据先看其jt,省略判空
			if(row.getAs("jt").equals("exhibition")){
				//说明没有展板轨迹信息
				if(tempList.size() == 0 ){
					tempList.add(row);
				}else{
					//看上一条的 jc 是不是和自己的exh 相同
					if(tempList.get(tempList.size()-1).getAs("jc").equals(row.getAs("exh"))){
						//如果相同,说明行为是连续的,继续分析下一条
						tempList.add(row);
					}else{
						//不相同说明不是一条行为轨迹
						//先将之前tempList 的数据进行记录,说明是一个行为轨迹
						//先判断上一步 jc 是否是 exhibition ,如果是就记录,如果不是,说明已经被记录了,就不再冗余记录
						if(!"".equals(lastJc) && "exhibition".equals(lastJc)){
							resultList.add(tempList.toArray());
						}
						//需要将tempList从后往前依次判断,循环或者递归
						if(tempList.size() == 1){
							//如果tempList只有一条数据,那么直接删除加入新的
							tempList.clear();
							tempList.add(row);
						}else{
							//有多条就进行循环匹配
							int num = tempList.size()-2;
							A : for(int i = num ; i>=0 ; i--){
								//判断tempList 从后往前元素的 jc  和当前条的exh 是否一致
								if(tempList.get(i).getAs("jc").equals(row.getAs("exh"))){
									//一样的话就追加进去,替换后一条,结束循环
									tempList.remove(i+1);
									tempList.add(row);
									break A;
								}else{
									//不一样的话继续循环往前找,删除后一条
									tempList.remove(i+1);
								}
							}
						}
					}
				}
			}else{
				//跳转内容不是 exhibition的
				if(tempList.size() >0){
					//有父 展板  , 循环  看自己的 exh 和  tempList 的 jc 是否一致
					//因为在 group 或者 sku等中,不能直接跳转到 与其来源展板同级的其他展板
					//比较templist 最后一条与此条记录的关系
					if(tempList.get(tempList.size()-1).getAs("jc").equals(row.getAs("exh"))){
						// 连续   == 轨迹进行存储
						tempList.add(row);
						resultList.add(tempList.toArray());
						//移除刚才添加进的子内容
						tempList.remove(tempList.size()-1);
					}else{
						//不连续 == 从后往前循环比对
						//判断tempList 条数
						if(tempList.size() ==1){
							//只有一条而且还对不上,有可能丛搜索进入,也有可能退出了APP,然后从主页点到了某个子节点
							//如果上一条记录是exhibition.就保存刚才那一条数据
							if(!"".equals(lastJc) && "exhibition".equals(lastJc)){
								resultList.add(tempList.toArray());
							}
							//清空展板集合
							tempList.clear();
							//因为本条记录没有父展板,而且属于叶子节点,那么就直接记录
							tempList.add(row);
							resultList.add(tempList.toArray());
							tempList.clear();
							
						}else{
							//进入这行说明 tempList  倒数第一个不连续而且 集合元素多于两个
							//先判断上一步 jc 是否是 exhibition ,如果是就记录,如果不是,说明已经被记录了,就不再冗余记录
							if(!"".equals(lastJc) && "exhibition".equals(lastJc)){
								resultList.add(tempList.toArray());
							}
							//有多条,进行循环比对
							//先删除最后一条,因为第一条在上面已经匹配了而且匹配不上
							tempList.remove(tempList.size()-1);
							
							//匹配标记,从后往前着条进行匹配
							boolean matchFlag = false;
							int num = tempList.size()-1;
							B : for(int i = num ; i>=0 ; i--){
								//判断tempList 从后往前元素的 jc  和当前条的exh 是否一致
								if(tempList.get(i).getAs("jc").equals(row.getAs("exh"))){
									//一样的话就追加进去,结束循环
									tempList.add(row);
									//储存结果
									resultList.add(tempList.toArray());
									//删除刚刚添加的子节点
									tempList.remove(tempList.size()-1);
									matchFlag = true;
									break B;
								}else{
									//不一样的话继续循环往前找,删除后一条
									tempList.remove(i);
								}
							}
							
							//没有匹配上,保存此条map.说明是独立的一条记录
							if( !matchFlag ){
								tempList.clear();
								tempList.add(row);
								resultList.add(tempList.toArray());
								tempList.clear();
							}
						}
					}//
				}else{
					//没有父展板,直接轨迹加上这个
					tempList.add(row);
					resultList.add(tempList.toArray());
					tempList.clear();
				}
			}
			//最后刷新上一步的jt值
			lastJc = row.getAs("jt");
		}
		return resultList;
	}
}
