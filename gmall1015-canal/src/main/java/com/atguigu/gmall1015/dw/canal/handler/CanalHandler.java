package com.atguigu.gmall1015.dw.canal.handler;

import com.alibaba.otter.canal.protocol.CanalEntry;


import java.util.List;

public class CanalHandler {


    public static void handle(String tableName , CanalEntry.EventType eventType , List<CanalEntry.RowData> rowDataList){

            if("order_info".equals(tableName)&&eventType.equals(CanalEntry.EventType.INSERT)){
                if(rowDataList.size()>0){
                    for (CanalEntry.RowData rowData : rowDataList) { //循环行集
                        List<CanalEntry.Column> columnsList = rowData.getAfterColumnsList(); //得到列集合
                        for (CanalEntry.Column column : columnsList) {
                            System.out.println(column.getName() + ":::" + column.getValue());
                        }

                    }

                }


            }


    }
}
