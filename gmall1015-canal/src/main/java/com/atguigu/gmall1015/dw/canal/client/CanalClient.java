package com.atguigu.gmall1015.dw.canal.client;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.atguigu.gmall1015.dw.canal.handler.CanalHandler;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;

public class CanalClient {

    public static void main(String[] args) {
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop1", 11111), "example", "", "");
        while (true){
            //连接抓取数据
            canalConnector.connect();
            canalConnector.subscribe("gmall1015.order_info");
            Message message = canalConnector.get(100);
            //判断是否抓取到数据
            if(message.getEntries().size()==0){
                System.out.println("没有数据，等待5秒");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }else{
                for (CanalEntry.Entry entry :message.getEntries()) {
                    //只处理数据业务
                    if( entry.getEntryType()!= CanalEntry.EntryType.ROWDATA){
                        continue;
                    }

                    CanalEntry.RowChange rowChange=null;
                    try {
                         rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                    } catch (InvalidProtocolBufferException e) {
                        e.printStackTrace();
                    }
                    CanalHandler.handle(entry.getHeader().getTableName(),rowChange.getEventType(),rowChange.getRowDatasList());


                }





            }





        }


    }


}
