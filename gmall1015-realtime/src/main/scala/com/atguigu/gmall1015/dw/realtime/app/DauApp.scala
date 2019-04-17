package com.atguigu.gmall1015.dw.realtime.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.gmall1015.dw.common.constant.GmallConstant
import com.atguigu.gmall1015.dw.common.util.MyEsUtil
import com.atguigu.gmall1015.dw.realtime.bean.StartupLog
import com.atguigu.gmall1015.dw.realtime.util.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

object DauApp {

  def main(args: Array[String]): Unit = {
      val sparkConf: SparkConf = new SparkConf().setAppName("dau_app").setMaster("local[*]")
      val ssc = new StreamingContext(sparkConf,Seconds(5))
      val inputDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.TOPIC_STARTUP,ssc)
//    inputDstream.foreachRDD{rdd=>
//      println(rdd.map(_.value()).collect().mkString("\n"))
//    }



    //  1 把今天访问过一次的用户记录下来   2  之后如果用户再次访问，要进行过滤
    // 1转换结构 把日期进行变换整理
    val startupLogDstream: DStream[StartupLog] = inputDstream.map { record =>
      val startupLogjson: String = record.value()
     // println(startupLogjson)
      val startupLog: StartupLog = JSON.parseObject(startupLogjson, classOf[StartupLog])
      val dateString: String = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(startupLog.ts))
      var dateArray = dateString.split(" ");
      startupLog.logDate = dateArray(0)
      val datetime = dateArray(1)
      startupLog.logHour = datetime.split(":")(0)
      startupLog.logHourMinute = startupLog.logHour + ":" + datetime.split(":")(1)
      startupLog
    }


   // 2 用户的每次启动，进行过滤，凡是已经当日启动过的用户，当日不在收录启动行为
    val filteredDStream: DStream[StartupLog] = startupLogDstream.transform { rdd =>
      rdd.cache()

      println("过滤前："+rdd.count())
      val jedis: Jedis = new Jedis("hadoop1", 6379)
      val curDate: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
      val key = "dau:" + curDate
      val dauSet: util.Set[String] = jedis.smembers(key)
      val dauSetBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(dauSet)
      val filteredRdd: RDD[StartupLog] = rdd.filter { startupLog =>
        !dauSetBC.value.contains(startupLog.uid)
      }
      println("过滤后："+filteredRdd.count())
      filteredRdd
    }

    //把相同uid的日志通过分组取每组第一个的方式 获取下来 达到去重效果
   val startlogGroupbyUidDstream: DStream[(String, Iterable[StartupLog])] = filteredDStream.map(startupLog=>(startupLog.uid,startupLog)).groupByKey()
   val startLogDistinctDStream: DStream[StartupLog] = startlogGroupbyUidDstream.flatMap{case (uid,startupLogItr)=>startupLogItr.take(1)}


    //  3把今天访问过一次的用户记录下来
    //保存当日访问过的用户清单 =》 redis
    startLogDistinctDStream.foreachRDD{rdd=>
      rdd.foreachPartition{startuplogItr=>
        val list: List[StartupLog] = startuplogItr.toList
         println(list.mkString("\n"))
        val jedis: Jedis = new Jedis("hadoop1", 6379)
        // 设计 redis保存的key     key: dau:2019-04-15 value( set )  uid    命令： sadd
        list.foreach{startupLog=>
          val key="dau:"+startupLog.logDate
          jedis.sadd(key,startupLog.uid)
        }
        jedis.close()

        MyEsUtil.insertEsBulk(GmallConstant.ES_INDEX_DAU,list)



      }

    }

    ssc.start()
    ssc.awaitTermination()


  }

}
