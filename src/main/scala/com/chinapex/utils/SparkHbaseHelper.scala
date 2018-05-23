package com.chinapex.utils

/**
  * Created by sli on 17-9-11.
  */

import com.chinapex.helpers.{ConfigurationHelper, SparkHelper}
import com.chinapex.helpers.SparkHelper.sparkContext
import com.chinapex.helpers.SparkHelper.sparkSession
import com.chinapex.interfaces.ColumnInfo
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.hadoop.hbase.CellUtil._
import org.apache.spark.sql.types._

import scala.reflect.ClassTag

object SparkHbaseHelper {

  def getRDD[T: ClassTag](
                 tableName: String,
                 columnFamily: String,
                 qualifier: String,
                 startRow: String,
                 stopRow: String,
                 toT: Array[Byte] => T
               ): RDD[T] ={
    val rawRDD = getRawRDD(tableName, startRow, stopRow)
    rawRDD.map(data => {
      toT(data._2.getValue(columnFamily.getBytes(), qualifier.getBytes()))
    })
  }

  def getRawRDD(
              tableName: String,
              startRow: String,
              stopRow: String
            ): RDD[(ImmutableBytesWritable, Result)] = {
    val hConf = HBaseConfiguration.create()
    hConf.set(TableInputFormat.INPUT_TABLE, tableName)
    hConf.set("hbase.zookeeper.quorum", ConfigurationHelper.hbaseIp)
    hConf.set("hbase.zookeeper.property.clientPort", ConfigurationHelper.hbasePort)
    hConf.set("zookeeper.znode.parent", ConfigurationHelper.hbaseZnode)
    hConf.set(TableInputFormat.SCAN_ROW_START, startRow)
    hConf.set(TableInputFormat.SCAN_ROW_STOP, stopRow)
    sparkContext.newAPIHadoopRDD(hConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
  }

  def getRDD(
              tableName: String,
              columnFamily: String,
              qualifier: String,
              startRow: String,
              stopRow: String
            ): RDD[String] = {
    getRDD[String](tableName, columnFamily, qualifier, startRow, stopRow, (x: Array[Byte]) => Bytes.toString(x))
  }

  def getRDD[T: ClassTag](
              tableName: String,
              startRow: String,
              stopRow: String,
              toT: Result => T
            ): RDD[T] = {
    val rawRDD = getRawRDD(tableName, startRow, stopRow)
    rawRDD.map(data => {
      toT(data._2)
    })
  }

  def res2Map(res: Result): Map[String, Array[Byte]] = {
    val cells = res.rawCells()
    cells.map(cell => {
      (Bytes.toString(cloneQualifier(cell)), cloneValue(cell))
    }).toMap
  }

  def res2MapWithK(res: Result): (String, Map[String, Array[Byte]]) = {
    (Bytes.toString(res.getRow), res2Map(res))
  }

  def putRDD[A <: Any](
            tableName: String,
            columnFamily: String,
            data: RDD[(String, Map[String, A])]
            ): Unit = {
    putRDD(tableName, columnFamily, data, keyMap2Put)
  }

  def putRDD[T](
            tableName: String,
            columnFamily: String,
            data: RDD[T],
            t: (T, String) => (ImmutableBytesWritable, Put)
            ): Unit = {
    val hConf = HBaseConfiguration.create()
    hConf.set("hbase.zookeeper.quorum", ConfigurationHelper.hbaseIp)
    hConf.set("hbase.zookeeper.property.clientPort", ConfigurationHelper.hbasePort)
    hConf.set("zookeeper.znode.parent", ConfigurationHelper.hbaseZnode)
    hConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    hConf.set("mapreduce.output.fileoutputformat.outputdir", "./tmp")

    val job = Job.getInstance(hConf)
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])

    data.map(x => t(x, columnFamily)).saveAsNewAPIHadoopDataset(job.getConfiguration)
  }

  def keyMap2Put[A <: Any](in: (String, Map[String, A]), columnFamily: String): (ImmutableBytesWritable, Put) = {
    val p = new Put(Bytes.toBytes(in._1))
    in._2.map(y => {
      y._2 match {
        case s: String => (Bytes.toBytes(y._1), Bytes.toBytes(s))
        case i: Int => (Bytes.toBytes(y._1), Bytes.toBytes(i))
        case d: Double => (Bytes.toBytes(y._1), Bytes.toBytes(d))
        case l: Long => (Bytes.toBytes(y._1), Bytes.toBytes(l))
        case f: Float => (Bytes.toBytes(y._1), Bytes.toBytes(f))
        case b: Boolean => (Bytes.toBytes(y._1), Bytes.toBytes(b))
        case t => (Bytes.toBytes(y._1), Bytes.toBytes(t.toString))
      }
    }).foreach(kv => {
      p.addColumn(Bytes.toBytes(columnFamily), kv._1, kv._2)
    })
    (new ImmutableBytesWritable, p)
  }

  def getDF[A <: Any](
              rdd: RDD[A],
              columnInfo: List[ColumnInfo],
              t2Row: (A, List[ColumnInfo]) => Row,
              schema: StructType,
              session: Option[SparkSession]
              ): DataFrame = {
    val ss = session.fold(SparkHelper.sparkSession){x => x}
    ss.createDataFrame(rdd.map(z => t2Row(z, columnInfo)), schema)
  }

  def getDF(
           rdd: RDD[Map[String, Array[Byte]]],
           columnInfo: List[ColumnInfo],
           session: Option[SparkSession]
           ): DataFrame = {
    val schema = columnInfo2Schema(columnInfo)
    getDF[Map[String, Array[Byte]]](rdd, columnInfo, rawMap2Row, schema, session)
  }

  def getDFWithK(
           rdd: RDD[(String, Map[String, Array[Byte]])],
           columnInfo: List[ColumnInfo],
           session: Option[SparkSession]
           ): DataFrame = {
    val newColumnInfo = ColumnInfo("", "ROWKEY") :: columnInfo
    val schema = columnInfo2Schema(newColumnInfo)
    getDF[(String, Map[String, Array[Byte]])](rdd, newColumnInfo, rawMapWithK2Row, schema, session)
  }


  //Should you need original row key to be added to dataframe, prepend a `ROWKEY` typed columnInfo to the list.
  def columnInfo2Schema(
                       columnInfo: List[ColumnInfo]
                       ): StructType = {
    StructType(
      columnInfo.map(z => ColumnInfo(z.columnName.replaceAll("`", ""), z.columnType)).map(ci => {
        ci.columnType match {
          case "String" => StructField(ci.columnName, StringType)
          case "Int" => StructField(ci.columnName, IntegerType)
          case "Double" => StructField(ci.columnName, DoubleType)
          case "Boolean" => StructField(ci.columnName, BooleanType)
          case "Float" => StructField(ci.columnName, FloatType)
          case "Long" => StructField(ci.columnName, LongType)
          case "Short" => StructField(ci.columnName, ShortType)
          case "ROWKEY" => StructField("ROWKEY", StringType)
          case _ => StructField(ci.columnName, NullType)
        }
      })
    )
  }

  def rawMapWithK2Row(
                     data:(String, Map[String, Array[Byte]]),
                     columnInfo: List[ColumnInfo]
                     ): Row = {
    Row.fromSeq(columnInfo.map(z => ColumnInfo(z.columnName.replaceAll("`", ""), z.columnType)).map(x => {
      x.columnType match {
        case "String" => Bytes.toString(data._2.getOrElse(x.columnName, Bytes.toBytes("")))
        case "Int" => Bytes.toInt(data._2.getOrElse(x.columnName, Bytes.toBytes(0)))
        case "Double" => Bytes.toDouble(data._2.getOrElse(x.columnName, Bytes.toBytes(0.0d)))
        case "Long" => Bytes.toLong(data._2.getOrElse(x.columnName, Bytes.toBytes(0l)))
        case "Float" => Bytes.toFloat(data._2.getOrElse(x.columnName, Bytes.toBytes(0.0f)))
        case "Short" => Bytes.toFloat(data._2.getOrElse(x.columnName, Bytes.toBytes(0.toShort)))
        case "Boolean" => Bytes.toBoolean(data._2.getOrElse(x.columnName, Bytes.toBytes(false)))
        case "ROWKEY" => data._1
        case z => ""
      }
    }))
  }

  def rawMap2Row(
                  data: Map[String, Array[Byte]],
                  columnInfo: List[ColumnInfo]
                ): Row = {
    Row.fromSeq(columnInfo.map(z => ColumnInfo(z.columnName.replaceAll("`", ""), z.columnType)).map(x => {
      x.columnType match {
        case "String" => Bytes.toString(data.getOrElse(x.columnName, Bytes.toBytes("")))
        case "Int" => Bytes.toInt(data.getOrElse(x.columnName, Bytes.toBytes(0)))
        case "Double" => Bytes.toDouble(data.getOrElse(x.columnName, Bytes.toBytes(0.0d)))
        case "Long" => Bytes.toLong(data.getOrElse(x.columnName, Bytes.toBytes(0l)))
        case "Float" => Bytes.toFloat(data.getOrElse(x.columnName, Bytes.toBytes(0.0f)))
        case "Short" => Bytes.toFloat(data.getOrElse(x.columnName, Bytes.toBytes(0.toShort)))
        case "Boolean" => Bytes.toBoolean(data.getOrElse(x.columnName, Bytes.toBytes(false)))
        case z => ""
      }
    }))
  }

}
