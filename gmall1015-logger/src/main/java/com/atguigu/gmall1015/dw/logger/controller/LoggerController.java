package com.atguigu.gmall1015.dw.logger.controller;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall1015.dw.common.constant.GmallConstant;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

@RestController    //  == @Controller+@ResponseBody        //标识 返回值是一个字符串
public class LoggerController {

    /***
     *  1  加一个时间戳
     *  2  落盘日志
     *  3  分流日志
     *  4  发送kafka
     *
     * @param log
     * @return
     */

    @Autowired
    KafkaTemplate<String,String> kafkaTemplate;

    private static final  org.slf4j.Logger logger = LoggerFactory.getLogger(LoggerController.class) ;
    @PostMapping("/log")
    public String doLog(@RequestParam("log") String log){
        JSONObject logJsonObj = JSON.parseObject(log);
        logJsonObj.put("ts",System.currentTimeMillis());
        logger.info(logJsonObj.toJSONString());

        if( "startup".equals(logJsonObj.getString("type")) ){
            kafkaTemplate.send(GmallConstant.TOPIC_STARTUP,logJsonObj.toJSONString());
        }else{
            kafkaTemplate.send(GmallConstant.TOPIC_EVENT,logJsonObj.toJSONString());
        }

        return "success";
    }

}
