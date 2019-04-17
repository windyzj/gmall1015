package com.atguigu.gmall1015.dw.common.util

import java.util
import java.util.Objects

import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, BulkResult, Index}

object MyEsUtil {
  private val ES_HOST = "http://hadoop1"
  private val ES_HTTP_PORT = 9200
  private var factory: JestClientFactory = null

  /**
    * 获取客户端
    *
    * @return jestclient
    */
  def getClient: JestClient = {
    if (factory == null) build()
    factory.getObject
  }

  /**
    * 关闭客户端
    */
  def close(client: JestClient): Unit = {
    if (!Objects.isNull(client)) try
      client.shutdownClient()
    catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }

  /**
    * 建立连接
    */
  private def build(): Unit = {
    factory = new JestClientFactory
    factory.setHttpClientConfig(new HttpClientConfig.Builder(ES_HOST + ":" + ES_HTTP_PORT).multiThreaded(true)
      .maxTotalConnection(20) //连接总数
      .connTimeout(10000).readTimeout(10000).build)

  }


  def main(args: Array[String]): Unit = {


    val jest: JestClient = getClient

    val value = "{\n  \"mid\":\"mid_1234\",\n  \"uid\":\"user_123\"\n}"
    val startup1 = Startup("mid777","uid222","","","","","","","","","",123124141)
    val startup2 = Startup("mid888","uid222","","","","","","","","","",123124141)
    val startups = List(startup1,startup2)

    val bulkBuilder: Bulk.Builder = new Bulk.Builder().defaultIndex("gmall1015_dau").defaultType("_doc")
    for (startup <- startups ) {
      val index: Index = new Index.Builder(startup).build()
      bulkBuilder.addAction(index)
    }



    //写入操作  单次   //保存位置：什么索引 ，什么type
   jest.execute(bulkBuilder.build())

    close(jest )

  }

  def insertEsBulk(index:String,sourceItr: Iterable[Any]): Unit ={
    val bulkBuilder: Bulk.Builder = new Bulk.Builder().defaultIndex(index).defaultType("_doc")
    for (source <- sourceItr ) {
      val index: Index = new Index.Builder(source).build()
      bulkBuilder.addAction(index)
    }
    val jest: JestClient = getClient
    val items: util.List[BulkResult#BulkResultItem] = jest.execute(bulkBuilder.build()).getItems
    close(jest)
    println("已保存："+items.size())
  }



  case class Startup(mid:String,
                     uid:String,
                     appid:String,
                     area:String,
                     os:String,
                     ch:String,
                     logType:String,
                     vs:String,
                     var logDate:String,
                     var logHour:String,
                     var logHourMinute:String,
                     var ts:Long
                    ) {

  }
}