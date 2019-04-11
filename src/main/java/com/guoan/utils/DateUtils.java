package com.guoan.utils;

import java.text.SimpleDateFormat;
import java.util.Date;

public class DateUtils {

    /**
     * Title: getDate
     * Description:  获取当前日期之前某天
     * @param arg
     * @return
     */
    public static String getDate(String arg){

        SimpleDateFormat simpleSdf = new SimpleDateFormat("yyyy-MM-dd");
        try{
            arg = (arg == null || arg == "null" ) ? "1" : arg;
        }catch (Exception e){
            arg = "1";
            e.printStackTrace();
        }finally {
            String yesterday = simpleSdf.format(  new Date(new Date().getTime()-(24*60*60*1000)*Integer.parseInt(arg)));
            return yesterday;
        }
    }
}
