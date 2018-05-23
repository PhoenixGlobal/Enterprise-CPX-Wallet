package com.chinapex.service

import java.util.UUID

import akka.actor.ActorRef
import com.chinapex.helpers.{ConfigurationHelper, SparkHelper}
import com.chinapex.helpers.SparkHelper.{sparkContext, sparkSession}
import com.chinapex.interfaces.{ColumnInfo, CreateSqlTableResult}
import com.chinapex.tools.dbclients.HBaseClient
import com.chinapex.utils.SparkHbaseHelper
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.spark.sql.Row
import org.slf4j.LoggerFactory
import org.apache.spark.sql.SparkSession

/**
  * Created by sli on 17-9-21.
  */
object SqlService {

  val logger = LoggerFactory.getLogger(this.getClass)

  val orgSessionMap = collection.mutable.Map.empty[String, SparkSession]

  def saveSqlToTalbe(sql: String, id: String, org: String, userName: String, actr: ActorRef): Unit = {
    try {
      logger.info(s"Received save sql to table message. start processing. id = $id, sql = $sql")
      val hiveContext = SparkHelper.hiveContext
      sparkContext.hadoopConfiguration.set("hbase.zookeeper.quorum", ConfigurationHelper.hbaseIp)
      sparkContext.hadoopConfiguration.set("hbase.zookeeper.preperty.clientPort", "2181")

      val dataFrame = hiveContext.sql(sql)

      val columns = dataFrame.columns.filterNot(_.equals("rowkey"))
      val firstRow = dataFrame.first().getValuesMap(columns).asInstanceOf[Map[String, Any]]
      var columnNameWithType: Map[String, String] = Map()
      columnNameWithType = firstRow.map(x => {
        x._2 match {
          case _: String => (x._1, "String")
          case _: Int => (x._1, "Int")
          case _: Long => (x._1, "Long")
          case _: Double => (x._1, "Double")
          case _ => (x._1, "String")
        }
      })

      val hbaseTableName = id
      sparkContext.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE, hbaseTableName)
      val hbaseClient = new HBaseClient(org)
      hbaseClient.createTable(hbaseTableName, ConfigurationHelper.sqlCreatedTableColumnFamily)
      SparkHbaseHelper.putRDD(org + ":" + hbaseTableName, ConfigurationHelper.sqlCreatedTableColumnFamily, dataFrame.rdd.map(row => row2MapWithK(row, columns)))
      logger.info(s"Done loading data into Hbase, sending message to notify Platform actor to do follow up procedure....")
      actr ! CreateSqlTableResult(id, true, org, columnNameWithType)
    } catch {
      case x: Throwable => {
        logger.error(s"There has been an error trying to create new table from sql. reason = ${x.getMessage} \n ${x.printStackTrace()}")
        actr ! CreateSqlTableResult(id, false, org, Map())
      }
    }
  }

  def row2MapWithK(row: Row, columns: Array[String]): (String, Map[String, Any]) = {
    val data = row.getValuesMap(columns).asInstanceOf[Map[String, Any]]
    val key = UUID.randomUUID().toString
    val dataMap = data.map(x => {
      //          accumulators.add(1)
      x._2 match {
        case s: String => (x._1, s)
        case i: Int => (x._1, i.toString)
        case d: Double => (x._1, d.toString)
        case l: Long => (x._1, l.toString)
        case _ => (x._1, "")
      }
    })
    (key, dataMap)
  }

  def initSessions(org: String): SparkSession = {
    val session = sparkSession.newSession()
    orgSessionMap += org -> session
    session
  }

  def loadTable(org: String, tableName: String, hbaseTable: String, columnInfo: List[ColumnInfo]): Unit = {
    val session = orgSessionMap.get(org).fold(initSessions(org))(x => x)
    val startRow = "0"
    val stopRow = "~~~~~~~~~~~~~~~~~~~~~~~~~~"
    val hTable = org + ":" + hbaseTable
    val rdd = SparkHbaseHelper.getRDD(hTable, startRow, stopRow, SparkHbaseHelper.res2MapWithK)
    SparkHbaseHelper.getDFWithK(rdd, columnInfo, Some(session)).createOrReplaceTempView(tableName)
    logger.warn(s"columnInfo: ${columnInfo.toString()}")
    logger.warn(s"Done load table, name: $tableName, hTable: $hbaseTable.")
  }

  def runSql(org: String, sql: String): Array[Row] = {
    val session = orgSessionMap.get(org).fold(initSessions(org))(x => x)
    val ret = session.sql(sql).take(50)
    ret.foreach(println)
    ret
  }

}
