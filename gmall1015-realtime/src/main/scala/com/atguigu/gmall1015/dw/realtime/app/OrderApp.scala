package com.atguigu.gmall1015.dw.realtime.app

import com.alibaba.fastjson.JSON
import com.atguigu.gmall1015.dw.common.constant.GmallConstant
import com.atguigu.gmall1015.dw.common.util.MyEsUtil
import com.atguigu.gmall1015.dw.realtime.bean.OrderInfo
import com.atguigu.gmall1015.dw.realtime.util.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object OrderApp {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("order_app")
     val ssc = new StreamingContext(sparkConf,Seconds(5))
     val inputDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.TOPIC_ORDER,ssc)

//      inputDstream.map(_.value()).foreachRDD{rdd=>
//        println(rdd.collect().mkString("\n"))
//      }
    val orderInfoDstream: DStream[OrderInfo] = inputDstream.map { record =>
      val jsonString: String = record.value()
      val orderInfo: OrderInfo = JSON.parseObject(jsonString, classOf[OrderInfo])
      // 脱敏
      orderInfo.consignee = orderInfo.consignee.splitAt(1)._1 + "**"
      orderInfo.consigneeTel = orderInfo.consigneeTel.splitAt(3)._1 + "****" + orderInfo.consigneeTel.splitAt(7)._2

      // 补充日期
      val createTimeArray: Array[String] = orderInfo.createTime.split(" ")
      orderInfo.createDate = createTimeArray(0)
      orderInfo.createHour = createTimeArray(1).split(":")(0)
      orderInfo.createHourMinute = createTimeArray(1).split(":")(0) + ":" + createTimeArray(1).split(":")(1)
      orderInfo
    }
    orderInfoDstream


    orderInfoDstream.foreachRDD{rdd=>

      rdd.foreachPartition{orderInfoItr=>
        //保存到ES中
        MyEsUtil.insertEsBulk(GmallConstant.ES_INDEX_NEW_ORDER,orderInfoItr.toList)

      }
    }

    ssc.start()
    ssc.awaitTermination()
  }

}
