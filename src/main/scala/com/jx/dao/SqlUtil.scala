package com.jx.dao

import com.jx.dao.es.ElasticsearchUtils
import com.jx.dao.sql.SqlUtilImpl
import com.jx.model.Datas.EsTableName
import com.jx.util.ElasticsearchUtils
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}

trait SqlUtil {


  /**
    * 查询
    * @param sql
    * @return
    */
  def queryAll(sql:String): List[Map[String, Any]] ={
    SqlUtilImpl.queryAll(sql)
  }

//  def insert()

  /**
    *
    * @param spark
    * @param esTableName
    * @return
    */
/*
  def queryModel(spark:SparkSession, esTableName: EsTableName): DataFrame ={
    ElasticsearchUtils.queryModel(sqlContext,esTableName)
  }
*/



}
