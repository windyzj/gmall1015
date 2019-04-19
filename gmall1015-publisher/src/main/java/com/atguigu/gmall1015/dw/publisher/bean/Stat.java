package com.atguigu.gmall1015.dw.publisher.bean;


import java.util.List;

/**
 * 代表一个饼图
 */
public class Stat {

    String title;

    List<Option> options;

    public Stat(String title, List<Option> options) {
        this.title = title;
        this.options = options;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public List<Option> getOptions() {
        return options;
    }

    public void setOptions(List<Option> options) {
        this.options = options;
    }
}
