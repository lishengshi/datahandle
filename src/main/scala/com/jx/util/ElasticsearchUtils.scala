package com.jx.util

import com.jx.model.Datas.EsTableName
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark.rdd.EsSpark
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

  private def indexTypeName(esTableName: EsTableName): String ={
    val index: String = esTableName.index
    val typeName: String = esTableName.typeName
    val indexType = index+"/"+typeName
    indexType
  }

  def insert(rDD: RDD[_],esTableName: EsTableName): Unit ={
    val id: String = esTableName.id
    val indexType: String = this.indexTypeName(esTableName)
    if (id.isEmpty){
      EsSpark.saveJsonToEs(rDD,indexType)
    }else{
      EsSpark.saveJsonToEs(rDD,indexType,Map("es.mapping.id"->id))
    }
  }

  def insertByJson(sparkSession: SparkSession,esTableName: EsTableName,dataJson:Seq[String]): Unit ={
    val rdd: RDD[String] = sparkSession.sparkContext.makeRDD(dataJson)
    this.insert(rdd,esTableName)
  }

  def insertByMap(sparkSession: SparkSession,esTableName: EsTableName,dataMap:Seq[Map[String,Any]]): Unit ={
    val rdd: RDD[Map[String, Any]] = sparkSession.sparkContext.makeRDD(dataMap)
    val indexType: String = this.indexTypeName(esTableName)
    val id: String = esTableName.id
    if (id.isEmpty){
      EsSpark.saveToEs(rdd,indexType)
    }else{
      EsSpark.saveToEs(rdd,indexType,Map("es.mapping.id"->id))
    }
  }


  def query(sparkSession: SparkSession,esTableName: EsTableName,dataJson:String): Unit ={
    val sc: SparkContext = sparkSession.sparkContext
    val indexType: String = this.indexTypeName(esTableName)
    EsSpark.esRDD(sc,indexType,dataJson)
  }



}
