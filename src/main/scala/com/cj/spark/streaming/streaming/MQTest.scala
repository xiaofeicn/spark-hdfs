package com.cj.spark.streaming.streaming


import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.rabbitmq.RabbitMQUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.cj.util.ConfigerHelper
import com.cj.util.DBHelper.getPropByEnv
import com.alibaba.fastjson.JSON.parseObject
import com.cj.spark.streaming.models.tb_personal_development
import org.apache.spark.SparkContext


object MQTest {

  def main(args: Array[String]): Unit = {

    val spark_ui_port=ConfigerHelper.getProperty("spark.ui.port.test.local").toInt
    val spark: SparkSession = SparkSession.builder()
      .appName("ttttt")
      .master("local[2]")
      .config("spark.ui.port",spark_ui_port)
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    val rdd=sc.textFile("hdfs://cj-data1:8020/cj-test/original-data/2019-03-28/20190328_868c69f4479242f8bb377fe15352153f.json")
    println(rdd.count())
    val json_name_dataARR = rdd.map(x => parseObject(x)).map(x => x.getJSONArray("RecordList").toArray().map(x => parseObject(x.toString)))
      .flatMap(x =>
        x.map(s => (s.getString("fStr_TableName"), s.getJSONArray("RecordList").toArray()))
      ).cache()
    val personal_development = json_name_dataARR.filter(_._1 == "tb_tech_personal_development")
      .flatMap(x => x._2)
      .map(line => parseObject(line.toString)).map(json => (
      json.getString("fStr_CourseID"),
      json.getString("fStr_StudentID"),
      json.getString("fStr_SubjectName"),
      json.getString("fStr_Json"),
      json.getString("fDtt_CreateTime"),
      json.getString("fDtt_ModifyTime")
    ))
//      .filter(!_._1.equals("3496ef4b129447d5beaf237f294f669f")).filter(!_._1.equals("4fefcbc42e6b462f8dc301ca47b2f07f"))

    import spark.implicits._
    val prop = getPropByEnv("test")
    val url="jdbc:mysql://rm-wz95c58i9ixyqw206o.mysql.rds.aliyuncs.com:3306/db_online_log?useSSL=false"
    personal_development.map(line => tb_personal_development(
      line._1,
      line._2,
      line._3,
      line._4,
      line._5,
      line._6)).toDF().write.mode("append").jdbc(url, "tb_personal_development", prop)



  }
}
