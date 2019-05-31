package com.jx.util

import java.sql.Connection
import java.util.Properties

import com.alibaba.druid.pool.DruidDataSource
import org.slf4j.{Logger, LoggerFactory}

/**
  * 基于druid实现的mysql连接池
  */
class MyConnectionPool
object MyConnectionPool {
  private val logger: Logger = LoggerFactory.getLogger(classOf[MyConnectionPool].getName)
  private var ds: DruidDataSource = null
  def init(): Unit ={
    try{
      val properties: Properties = PropertiesUtils.apply()
      val className: String = properties.getProperty("jx.mysql.classname")
      val jdbcUrl: String = properties.getProperty("jx.mysql.jdbcurl")
      val user: String = properties.getProperty("jx.mysql.user")
      val password: String = properties.getProperty("jx.mysql.password")
      ds = new DruidDataSource()
      ds.setDriverClassName(className)
      ds.setUrl(jdbcUrl)
      ds.setUsername(user)
      ds.setPassword(password)
      ds.setInitialSize(15)
      ds.setMaxActive(50)
      ds.setMinIdle(10)
      ds.setMaxWait(500)
    }catch{
      case e:Exception =>logger.info(e.getMessage)
    }
  }

  def getConnection(): Connection ={
    try{
      ds.getConnection.getConnection
    }catch{
      case e:Exception=>e.getMessage
        null
    }
  }

  def returnConnection(connection:Connection): Unit ={
    if (!connection.isClosed){
      connection.close()
    }
  }

  def closeConnection(connection:Connection,dataSource:DruidDataSource): Unit ={
    if (connection!=null){
      connection.close()
    }
    if (dataSource!=null){
      dataSource.close()
    }
  }

}
