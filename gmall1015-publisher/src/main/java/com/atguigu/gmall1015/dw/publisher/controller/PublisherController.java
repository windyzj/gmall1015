package com.atguigu.gmall1015.dw.publisher.controller;


import com.alibaba.fastjson.JSON;
import com.atguigu.gmall1015.dw.publisher.service.PublisherService;
import org.apache.commons.lang.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

@RestController
public class PublisherController {

    @Autowired
    PublisherService publisherService;

    @GetMapping("realtime-total")
    public String  getRealtimeTotal(@RequestParam("date") String date){

        ArrayList<Map> totalList = new ArrayList();

        //日活
        HashMap dauMap = new HashMap();
        dauMap.put("id","dau");
        dauMap.put("name","新增日活");
        Integer dauTotal = publisherService.getDauTotal(date);
        dauMap.put("value",dauTotal);
        totalList.add(dauMap);

        //新增用户
        HashMap newUserMap = new HashMap();
        newUserMap.put("id","new_mid");
        newUserMap.put("name","新增用户");
        newUserMap.put("value",1200);
        totalList.add(newUserMap);


        return JSON.toJSONString(totalList) ;
    }

    @GetMapping("realtime-hour")
    public  String getRealtimeHour(@RequestParam("id") String id, @RequestParam("date") String date){

        if("dau".equals(id)){
            //今日分时统计
            Map dauTdMap = publisherService.getDauMap(date);
            //昨日分时统计
            String yesterday = getYesterday(date);
            Map dauYdMap = publisherService.getDauMap(yesterday);

            //整理合并
            Map<String ,Map>  dauMap=new HashMap();
            dauMap.put("today",dauTdMap);
            dauMap.put("yesterday",dauYdMap);

            return JSON.toJSONString(dauMap);
        }
        return null;
    }

    private String getYesterday(String today){
        Date todayDt=null;
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        try {
              todayDt =simpleDateFormat.parse(today);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        Date yesterdayDt = DateUtils.addDays(todayDt, -1);
        String yesterday = simpleDateFormat.format(yesterdayDt);
        return yesterday;
    }


}
