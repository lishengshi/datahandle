package com.jx.util

import scalikejdbc.ConnectionPool

object SqlConfigUtil {


  /**
    * 配置mysql连接
    * @param jdbcUrl
    * @param user
    * @param password
    */
  def readMysql(jdbcUrl:String,user:String,password:String): Unit ={
    val driver: String = "com.mysql.jdbc.Driver"
    Class.forName(driver)
    ConnectionPool.singleton(jdbcUrl,user,password)
  }

  def readMysql(): Unit ={
    val properties = PropertiesUtils.apply()
    val jdbcUrl: String = properties.getProperty("jx.mysql.jdbcurl")
    val user: String = properties.getProperty("jx.mysql.user")
    val password: String = properties.getProperty("jx.mysql.password")
    this.readMysql(jdbcUrl,user,password)
  }


}
