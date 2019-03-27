package com.cj.util

import java.io.InputStream
import java.util.Properties

/**
  * Created by USER on 2017/8/2.
  */
object ConfigerHelper {
  var properties=new Properties()
  val inputStream: InputStream = ConfigerHelper.getClass.getResourceAsStream("config.properties")

  properties.load(inputStream)

  def getProperty(key:String):String={
    properties.getProperty(key)
  }

}
