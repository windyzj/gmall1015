package com.atguigu.gmall1015.dw.publisher.controller;


import com.alibaba.fastjson.JSON;
import com.atguigu.gmall1015.dw.publisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
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

}
