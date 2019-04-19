package com.atguigu.gmall1015.dw.publisher.service;

import java.util.Map;

public interface PublisherService {

    public Integer  getDauTotal(String date);

    public Map getDauMap(String date);

    public Double  getOrderTotalAmount(String date);

    public Map  getOrderTotalAmountHourMap(String date);

    public Map  getSaleDetail(String date,String keyword,int startPage,int pagesize,String aggField,int aggSize);

}
