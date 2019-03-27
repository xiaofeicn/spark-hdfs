package com.cj.spark.streaming.streaming


import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.rabbitmq.RabbitMQUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.cj.util.ConfigerHelper
import com.cj.util.RabbitMqHelper.rabbitMqMap
import com.alibaba.fastjson.JSON.parseObject
import org.apache.spark.SparkContext


object MQTest {

  private[this] val checkpointDirectory = ConfigerHelper.getProperty("checkpointDirectory")
  private[this] val appName = ConfigerHelper.getProperty("appName.test")
  private[this] val hdfs_data_path = ConfigerHelper.getProperty("hdfs.data")
  val log: Logger = org.apache.log4j.LogManager.getLogger(appName)

  def createStreamingContext(appName: String): StreamingContext = {

    val spark: SparkSession = SparkSession.builder()
      .appName(appName)
      .master("local[2]")
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext

    val ssc: StreamingContext = new StreamingContext(sc, Seconds(10))
    val ds = ssc.textFileStream(hdfs_data_path)
    ds.foreachRDD(rdd => {
      rdd.foreach(str => {
        println(str)

      })

    })

    ssc
  }

  def main(args: Array[String]): Unit = {
    /**
      * 使用Checkpoint
      * val sct = StreamingContext.getOrCreate(thisCheckpointDirectory,
      * () => {
      * createStreamingContext(thisCheckpointDirectory, appName)
      * })
      *
      */
    //
    //    val sct = createStreamingContext(appName)
    //    sct.start()
    //    sct.awaitTermination()
    val map = Map('a' -> 1)

    println(10 / (map.getOrElse('b', 0) * 2))

  }
}
