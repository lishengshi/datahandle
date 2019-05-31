package com.jx.model

import org.apache.spark.sql.Row

/**
  * 映射的实体类
  * 数据模板
  */
object Datas extends Serializable {
  abstract class Data
  abstract class Table

  case class EsTable(index:String,typeName:String,columns:String,values:Seq[Any]) extends Table

  case class EsTableName(index:String,typeName:String,id:String=null) extends Table

  case class TableColumnMappingValue(tableName:String,columns:String,values:Array[Row]) extends Table


}
