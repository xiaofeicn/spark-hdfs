package com.cj.util

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}

/**
  * Created by USER on 2017/9/8.
  */
object DateHelper {
  final val timeFormater: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm")
  final val timeHourFormater: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
  final val timeDay: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
  final val timeHour: SimpleDateFormat = new SimpleDateFormat("HH")


  def toDay: String = {

    timeDay.format(new Date())

  }

}