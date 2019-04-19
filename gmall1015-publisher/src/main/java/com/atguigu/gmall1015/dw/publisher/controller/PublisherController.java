package com.atguigu.gmall1015.dw.publisher.controller;


import com.alibaba.fastjson.JSON;
import com.atguigu.gmall1015.dw.publisher.bean.Option;
import com.atguigu.gmall1015.dw.publisher.bean.SaleInfo;
import com.atguigu.gmall1015.dw.publisher.bean.Stat;
import com.atguigu.gmall1015.dw.publisher.service.PublisherService;
import org.apache.commons.lang.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.lang.model.element.VariableElement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

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


        //新增交易额
        HashMap orderAmountMap = new HashMap();
        orderAmountMap.put("id","order_amount");
        orderAmountMap.put("name","新增交易额");
        Double orderTotalAmount = publisherService.getOrderTotalAmount(date);
        orderAmountMap.put("value",orderTotalAmount);
        totalList.add(orderAmountMap);

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
        }else if("order_amount".equals(id)){
            //今日分时统计
            Map orderAmountTdMap = publisherService.getOrderTotalAmountHourMap(date);
            //昨日分时统计
            String yesterday = getYesterday(date);
            Map orderAmountYdMap = publisherService.getOrderTotalAmountHourMap(yesterday);

            //整理合并
            Map<String ,Map>  orderAmountMap=new HashMap();
            orderAmountMap.put("today",orderAmountTdMap);
            orderAmountMap.put("yesterday",orderAmountYdMap);

            return JSON.toJSONString(orderAmountMap);
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

    @GetMapping("sale_detail")
    public  String saleDetail(@RequestParam("date") String date,@RequestParam("startpage") int startPage,@RequestParam("size") int pageSize,@RequestParam("keyword") String keyword ){

        Map saleDetailMap = publisherService.getSaleDetail(date, keyword, startPage, pageSize, "user_gender", 2);
        Integer total =(Integer)saleDetailMap.get("total");
        Map genderMap  =(Map)saleDetailMap.get("aggMap");
        List<HashMap> detailList =(List<HashMap>)saleDetailMap.get("detail");

        //个数变为比例  //F M 变成男女
        List<Option> genderOptions=new ArrayList();
        for (Object o : genderMap.entrySet()) {
            Map.Entry entry = (Map.Entry) o;
            String key = (String)entry.getKey();
            Long count = (Long)entry.getValue();

            double genderRatio = Math.round(count * 1000D / total) / 10D;
            String genderKey = key.equals("F") ? "女" : "男";
            genderOptions.add(new Option(genderKey,genderRatio)) ;
        }
        Stat genderStat = new Stat("用户性别占比", genderOptions);

        Map saleDetailwithAgeMap = publisherService.getSaleDetail(date, keyword, startPage, pageSize, "user_age", 100);
        Map aggMap = (Map)saleDetailwithAgeMap.get("aggMap");

        Long age_20count=0L;
        Long age20_30count=0L;
        Long age30_count=0L;

        //根据年龄判断年龄段，分别累加
        for (Object o : aggMap.entrySet()) {
            Map.Entry entry = (Map.Entry) o;
            String agekey =(String) entry.getKey();
            Integer ageNum=Integer.parseInt(agekey);
            Long ageCount =(Long) entry.getValue();
            if(ageNum<20){
                age_20count+=ageCount;
            }else if(ageNum>=20&&ageNum<30){
                age20_30count+=ageCount;
            }else{
                age30_count+=ageCount;
            }
        }
        //分别求出比例
        Double age_20Ratio=Math.round( age_20count*1000D/total)/10D;
        Double age20_30Ratio=Math.round( age20_30count*1000D/total)/10D;
        Double age30_Ratio=Math.round( age30_count*1000D/total)/10D;

        List<Option> ageOptions=new ArrayList<>();
        ageOptions.add( new Option("20岁以下", age_20Ratio)) ;
        ageOptions.add( new Option("20岁到30岁", age20_30Ratio)) ;
        ageOptions.add( new Option("30岁及以上", age30_Ratio)) ;
        Stat ageStat = new Stat("用户年龄占比", ageOptions);

        //保存两个饼图
        List<Stat> statList=new ArrayList();
        statList.add(ageStat);
        statList.add(genderStat);

        SaleInfo saleInfo = new SaleInfo(total, statList, detailList);

        return  JSON.toJSONString(saleInfo);

    }



}
