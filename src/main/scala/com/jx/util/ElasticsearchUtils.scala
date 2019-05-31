package com.jx.util

import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

class ElasticsearchUtils
object ElasticsearchUtils {

  private val logger: Logger = LoggerFactory.getLogger(classOf[ElasticsearchUtils].getName)

  def init(appName:String,masterUrl:String):SparkSession={
    val properties = PropertiesUtils.apply()
    val host: String = properties.getProperty("jx.es.host")
    val port: Int = properties.getProperty("jx.es.port").trim.toInt
    val spark: SparkSession = SparkSession.builder()
      .enableHiveSupport()
      .appName(appName)
      .master(masterUrl)
      .config("es.index.auto.create", "true")
      .config("pushdown", "true")
      .config("es.nodes",host)
      .config("es.port", port)
      .config("es.nodes.wan.only", "true")
      .getOrCreate()
    SessionUtil.init(spark)
    spark
  }



}
