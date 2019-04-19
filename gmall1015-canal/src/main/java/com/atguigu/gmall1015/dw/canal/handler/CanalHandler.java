package com.atguigu.gmall1015.dw.canal.handler;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.atguigu.gmall1015.dw.canal.util.MyKafkaSender;
import com.atguigu.gmall1015.dw.common.constant.GmallConstant;
import com.google.common.base.CaseFormat;


import java.util.List;

public class CanalHandler {


    public static void handle(String tableName , CanalEntry.EventType eventType , List<CanalEntry.RowData> rowDataList){

            if("order_info".equals(tableName)&&eventType.equals(CanalEntry.EventType.INSERT)){
                if(rowDataList.size()>0){
                    for (CanalEntry.RowData rowData : rowDataList) { //循环行集
                        List<CanalEntry.Column> columnsList = rowData.getAfterColumnsList(); //得到列集合
                        JSONObject jsonObject = new JSONObject();
                        for (CanalEntry.Column column : columnsList) {
                          //  System.out.println(column.getName() + ":::" + column.getValue());
                            String propertyName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, column.getName());
                            jsonObject.put(propertyName,column.getValue());
                        }
                        //发送
                        MyKafkaSender.send(GmallConstant.TOPIC_ORDER,jsonObject.toJSONString());
                    }

                }


            }


    }
}
