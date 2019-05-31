package com.jx.util

import java.io.IOException
import java.util.Properties

import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

class PropertiesUtils
object PropertiesUtils {
  private val logger: Logger = LoggerFactory.getLogger(classOf[PropertiesUtils].getName)

  private val propertiesMap = new mutable.HashMap[String,Properties]()

  final val defaultPath = "resources/connection/connection.properties"

  def apply(path:String=defaultPath): Properties = {
    this.properties(path)
  }

  def properties(path:String): Properties ={
    val properties: Properties = propertiesMap.get(path).get
    if (properties==null){
      try{
        val pro = new Properties()
        pro.load(getClass.getResourceAsStream(path))
        propertiesMap.put(path,pro)
      }catch{
        case e:IOException => logger.info(e.getMessage)
      }
    }
    properties
  }

}
