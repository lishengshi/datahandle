package com.jx.util

import com.jx.util.StreamingContextUtil.logger
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/**
  * 单例
  * @author limeng
  * @date 2019/3/28 9:56
  * @version 1.0
  */
object SessionUtil {
  private var sparkSession: SparkSession = _
  private var sparkContext: SparkContext = _


  def init(sparkSession: SparkSession): Unit ={
    this.sparkSession = sparkSession
  }

  def getSparkSession: SparkSession = {
    if(null == sparkSession){
      logger.error("SessionUtil sparkSession instance empty")
    }
    sparkSession
  }

  def init(sparkContext:SparkContext): Unit ={
    this.sparkContext = sparkContext
  }


  def getSparkContext: SparkContext = {
    if(null == sparkContext){
      logger.error("SessionUtil sparkContext instance empty")
    }
    sparkContext
  }

}
