package com.jx.dao.sql

import com.jx.dao.SqlUtil
import com.jx.model.Datas.TableColumnMappingValue
import org.apache.spark.sql.Row
import org.slf4j.{Logger, LoggerFactory}
import scalikejdbc._

import scala.collection.mutable.ListBuffer

object SqlUtilImpl {

  private val logger: Logger = LoggerFactory.getLogger(classOf[SqlUtil].getName)

  /**
    * 查询所有
    * @param sql
    */
  def queryAll(sql:String): List[Map[String, Any]] ={
    var maps: List[Map[String, Any]] = null
    if (sql != null){
      maps = DB.readOnly(implicit session => {
        SQL(sql).map(_.toMap()).list().apply()
      })
    }
    maps
  }

  def queryOne(sql:String)={
    var maps: Map[String, Any] = null
    if (sql!=null){
      maps = DB.readOnly(implicit session => {
        SQL(sql).map(rs => rs.toMap()).single().apply()
      }).get
    }
    maps
  }

  def batchSave(tableColumnMappingValue:TableColumnMappingValue)={
    if (tableColumnMappingValue!=null){
      DB.localTx{implicit session=>
        val tableName: String = tableColumnMappingValue.tableName
        val columns: String = tableColumnMappingValue.columns
        val values: Array[Row] = tableColumnMappingValue.values
        val buffer: ListBuffer[Long] = new ListBuffer[Long]
        values.foreach(f=>{
          val f1: Seq[String] = f.toSeq.map(y=>y.toString)
          val res: Long = SQL(s"insert into ${tableName} ${columns} values ${f1.mkString(",")}").updateAndReturnGeneratedKey().apply()
          buffer += res
        })
        if (buffer.size==0){
          logger.info(s"insert into ${tableName} error")
        }
      }
    }
  }



}
