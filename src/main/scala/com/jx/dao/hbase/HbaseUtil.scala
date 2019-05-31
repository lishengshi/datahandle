package com.jx.dao.hbase

import com.jx.util.PropertiesUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.io.compress.Compression
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.io.{NullWritable, Writable}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * hbase的操作类
  */
object HbaseUtil {
  val conn: Connection = ConnectionFactory.createConnection(this.hbaseConf)
  val hbaseConf: Configuration = {
    val properties = PropertiesUtils.apply()
    val zkBroker: String = properties.getProperty("jx.zookeeper.list")
    val conf: Configuration = HBaseConfiguration.create()
    conf.set("zookeeper.znode.parent", "/hbase")
    if (zkBroker!=null) conf.set("hbase.zookeeper.quorum", zkBroker)
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf
  }

  def createTable(tableName:String,columnFamilys:String*): Unit ={
    val table: TableName = TableName.valueOf(tableName)
    val admin: Admin = conn.getAdmin
    if (admin.tableExists(table)){
      admin.disableTable(table)
      admin.deleteTable(table)
    }
    val nameDescriptor = new HTableDescriptor(table)
    for (family<-columnFamilys){
      nameDescriptor.addFamily(new HColumnDescriptor(family.getBytes).setCompactionCompressionType(
        Compression.Algorithm.GZ
      ))
    }
    admin.createTable(nameDescriptor)
  }

  /**
    * 读取hbase表的内容生成rdd
    * @param spark
    * @param tableName
    * @return
    */
  def tranHbaseTable2RDD(spark:SparkSession,tableName: String):RDD[(ImmutableBytesWritable, Result)]={
    val conf: Configuration = this.hbaseConf
    conf.set(TableInputFormat.INPUT_TABLE,tableName)
    val sc: SparkContext = spark.sparkContext
    val rdd: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(conf,classOf[TableInputFormat],classOf[ImmutableBytesWritable],classOf[Result])
    rdd
  }

  //TODO
  def tranHbaseRDD2DF(spark:SparkSession,keyName:String,qualifiers:Array[(String,String)], tableName: String)={
    val fields: Seq[StructField] = this.getSchemaFromColumns(keyName,qualifiers)


  }

  def getSchemaFromColumns(keyName:String,qualifiers:Array[(String,String)])={
    val fields: Array[StructField] = qualifiers.map {
      case (column, datatype) => {
        datatype match {
          case "String" => StructField(column.replace(":", "_"), StringType)
          case "Int" => StructField(column.replace(":", "_"), IntegerType)
          case "Double" => StructField(column.replace(":", "_"), DoubleType)
          case "Boolean" => StructField(column.replace(":", "_"), BooleanType)
          case _ => StructField(column.replace(":", "_"), StringType)
        }
      }
    }
    StructType(Array(StructField(keyName,StringType)))++fields
  }

  /**
    *
    * @param dataFrame 写入的df
    * @param tableName  hbase表名
    * @param keyName    hbase的
    * @param qualifiers
    */
  def bulkWriteFromDF(dataFrame:DataFrame,tableName:String,keyName:String,qualifiers:Array[(String,String)])={
    val conf: Configuration = this.hbaseConf
    val job: Job = Job.getInstance(conf)
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Writable])
    job.getConfiguration.set(TableOutputFormat.OUTPUT_TABLE,tableName)
    val rdd: RDD[(ImmutableBytesWritable, Put)] = dataFrame.rdd.map(row => {
      val put: Put = new Put(Bytes.toBytes(row.getAs[String](keyName)))
      qualifiers.foreach {
        case (column, dataType) => {
          val Array(family, col) = column.split(":")
          dataType match {
            case "String" =>
              val value: String = row.getAs[String](family + "_" + col)
              if (value != "") {
                put.addColumn(Bytes.toBytes(family), Bytes.toBytes(col), Bytes.toBytes(value))
              }
            case "Double" =>
              val value: Double = row.getAs[Double](s"${family}_${col}")
              if (value != null) {
                put.addColumn(Bytes.toBytes(family), Bytes.toBytes(col), Bytes.toBytes(value))
              }
            case "Int" =>
              val value: Int = row.getAs[Int](family + "_" + col)
              if (value!=null){
                put.addColumn(Bytes.toBytes(family),Bytes.toBytes(col),Bytes.toBytes(value))
              }
            case "Boolean" =>
              val value: Boolean = row.getAs[Boolean](family + "_" + col)
              if (value!=null){
                put.addColumn(Bytes.toBytes(family),Bytes.toBytes(col),Bytes.toBytes(value))
              }
            case "Long"=>
              val value: Long = row.getAs[Long](family + "_" + col)
              if (value!=null){
                put.addColumn(Bytes.toBytes(family),Bytes.toBytes(col),Bytes.toBytes(value))
              }
          }
        }
      }
      (new ImmutableBytesWritable, put)
    }).filter(!_._2.isEmpty)
    rdd.saveAsNewAPIHadoopDataset(job.getConfiguration)
  }

  def bulkWriteFromRDD(tableName:String)={
    val conf: Configuration = this.hbaseConf
    val job: Job = Job.getInstance(conf)
    job.setOutputFormatClass(classOf[TextOutputFormat[NullWritable]])
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[NullWritable])
    job.getConfiguration.set(TableOutputFormat.OUTPUT_TABLE,tableName)




  }









}
