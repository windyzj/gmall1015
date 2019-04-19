package com.atguigu.gmall1015.dw.publisher.bean;

/*
 代表饼图中的一个选项
 */
public class Option {

    public Option(String name, Double value) {
        this.name = name;
        this.value = value;
    }

    String name;

    Double value;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Double getValue() {
        return value;
    }

    public void setValue(Double value) {
        this.value = value;
    }
}
