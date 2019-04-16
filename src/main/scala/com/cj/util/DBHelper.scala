package com.cj.util

import java.util.Properties


object DBHelper {
  private[this] val driver = ConfigerHelper.getProperty("driver")
  private[this] val jdbcUserTest = ConfigerHelper.getProperty("jdbc.user.test")
  private[this] val jdbcPasswordTest = ConfigerHelper.getProperty("jdbc.password.test")
  private[this] val jdbcUser = ConfigerHelper.getProperty("jdbc.user")
  private[this] val jdbcPassword = ConfigerHelper.getProperty("jdbc.password")


  def getProp(isTest:Boolean): Properties ={
    val prop = new Properties()


    if (isTest) {
      prop.setProperty("user", jdbcUserTest)
      prop.setProperty("password", jdbcPasswordTest)
      prop.setProperty("driver", driver)
    } else {
      prop.setProperty("user", jdbcUser)
      prop.setProperty("password", jdbcPassword)
      prop.setProperty("driver", driver)
    }
    prop
  }
  def getPropByEnv(env:String): Properties ={
    val prop = new Properties()
    val jdbcUser = ConfigerHelper.getProperty(s"jdbc.user.$env")
    val jdbcPassword = ConfigerHelper.getProperty(s"jdbc.password.$env")

    prop.setProperty("user", jdbcUser)
    prop.setProperty("password", jdbcPassword)
    prop.setProperty("driver", driver)
    prop
  }

}
