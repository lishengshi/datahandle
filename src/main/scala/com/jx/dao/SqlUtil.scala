package com.jx.dao

import com.jx.dao.sql.SqlUtilImpl

trait SqlUtil {


  /**
    * 查询
    * @param sql
    * @return
    */
  def queryAll(sql:String): List[Map[String, Any]] ={
    SqlUtilImpl.queryAll(sql)
  }

  def insert




}
