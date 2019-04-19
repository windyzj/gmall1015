package com.atguigu.gmall1015.dw.publisher.bean;

import java.util.HashMap;
import java.util.List;

public class SaleInfo {

    Integer total;

    List<Stat> stat;

    List<HashMap> detail;

    public SaleInfo(Integer total, List<Stat> stat, List<HashMap> detail) {
        this.total = total;
        this.stat = stat;
        this.detail = detail;
    }

    public Integer getTotal() {
        return total;
    }

    public void setTotal(Integer total) {
        this.total = total;
    }

    public List<Stat> getStat() {
        return stat;
    }

    public void setStat(List<Stat> stat) {
        this.stat = stat;
    }

    public List<HashMap> getDetail() {
        return detail;
    }

    public void setDetail(List<HashMap> detail) {
        this.detail = detail;
    }




}
