package com.cj.util

object RabbitMqHelper {
  private[this] val hosts = ConfigerHelper.getProperty("hosts")
  private[this] val userName = ConfigerHelper.getProperty("userName")
  private[this] val password = ConfigerHelper.getProperty("passwordMQ")
  private[this] val routingKeys = ConfigerHelper.getProperty("routingKeys")
  private[this] val queueName = ConfigerHelper.getProperty("queueName")
  private[this] val exchangeName = ConfigerHelper.getProperty("exchangeName")
  private[this] val exchangeType = ConfigerHelper.getProperty("exchangeType")
  private[this] val vHost = ConfigerHelper.getProperty("vHost")
  private[this] val port = ConfigerHelper.getProperty("port")

  private[this] val hostsTest = ConfigerHelper.getProperty("hosts.test")
  private[this] val userNameTest = ConfigerHelper.getProperty("userName.test")
  private[this] val passwordTest = ConfigerHelper.getProperty("passwordMQ.test")
  private[this] val routingKeysTest = ConfigerHelper.getProperty("routingKeys.test")
  private[this] val queueNameTest = ConfigerHelper.getProperty("queueName.test")
  private[this] val exchangeNameTest = ConfigerHelper.getProperty("exchangeName.test")
  private[this] val exchangeTypeTest = ConfigerHelper.getProperty("exchangeType.test")
  private[this] val vHostTest = ConfigerHelper.getProperty("vHost.test")

  def rabbitMqMap(isTest:Boolean): Map[String, String] ={
    var map:Map[String,String]=null
    if (isTest){
      map=Map(
        "hosts" -> hostsTest,
        "queueName" -> queueNameTest,
        "exchangeName" -> exchangeNameTest,
        "port" -> port,
        "exchangeType" -> exchangeTypeTest,
        "virtualHost" -> vHostTest,
        "userName" -> userNameTest,
        "password" -> passwordTest,
        "routingKeys" -> routingKeysTest
      )
    }else{
      map=Map(
        "hosts" -> hosts,
        "queueName" -> queueName,
        "exchangeName" -> exchangeName,
        "port" -> port,
        "exchangeType" -> exchangeType,
        "virtualHost" -> vHost,
        "userName" -> userName,
        "password" -> password,
        "routingKeys" -> routingKeys
      )
    }
    map

  }
}
