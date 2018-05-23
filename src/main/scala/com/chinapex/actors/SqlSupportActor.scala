package com.chinapex.actors

import java.util.UUID

import akka.actor.{Actor, ActorRef}
import com.chinapex.helpers.{ConfigurationHelper, SparkHelper}
import org.slf4j.LoggerFactory
import com.chinapex.interfaces._
import com.chinapex.helpers.SparkHelper.sparkContext
import com.chinapex.helpers.SparkHelper.sqlContext
import org.apache.spark.sql.hive.HiveContext
import com.chinapex.tools.dbclients.HBaseClient
import com.chinapex.service.impl.HiveTableServiceImpl
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.types._

import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.Row
import com.chinapex.helpers.SparkHelper.sqlContext.implicits._
import com.chinapex.service.SqlService
import com.chinapex.utils.SparkHbaseHelper



/**
  * Created by sli on 17-8-29.
  */

class SqlSupportActor extends Actor {
  val logger = LoggerFactory.getLogger(this.getClass)


  override def receive: Receive = {
    case SaveSqlToTableByChild(sql, id, org, userName, actr) => {
      SqlService.saveSqlToTalbe(sql, id, org, userName, actr)
    }
    case PreviewTableWithSqlByChild(sql, id, actr) => {
      try {
        logger.info(s"Start trying to get preview of table.")
        val hiveContext = SparkHelper.hiveContext
        val dataFrame = hiveContext.sql(sql)
        val columns = dataFrame.columns.filterNot(_.equals("rowkey"))
        val result = dataFrame.collect().map(row => row.getValuesMap(columns).asInstanceOf[Map[String, Any]])
        actr ! PreviewTableWithSqlResult(id, true, result)
      } catch {
        case x: Any => {
          logger.error(s"Error trying to get preview of table. ${x.getMessage}, ${x.printStackTrace()}.")
          actr ! PreviewTableWithSqlErrorResult(id, false, x.getMessage)
        }
      }

    }
    case CreateHiveExternalTableByChild(sql, tableName, actr) => {
      try {
        HiveTableServiceImpl.createTable(sql)
        actr ! CreateHiveExternalTableResult(true, "")
      } catch {
        case x: Throwable => {
          //          logger.error(s"There has been an error trying to create external table in hive. reason = ${x.getMessage}, stack = \n ${x.printStackTrace()}")
          actr ! CreateHiveExternalTableResult(false, x.getMessage)
        }
      }

    }

    case RefreshMasterTableByChild(tableName, recreateSQL, actr) =>
      // 1. drop master table
      // 2. recreate it using "recreateSQL"
      try {
        HiveTableServiceImpl.deleteTableByName(tableName)
        val tables = HiveTableServiceImpl.listTables()
        if (tables.contains(tables)) {
          logger.info(s"DataService sqlSupportActor: Master table: ${tableName} dropped, but still exist, wait 3 seconds...")
          Thread.sleep(3000)
        }
        HiveTableServiceImpl.createTable(recreateSQL)
        actr ! CreateHiveExternalTableResult(true, "")

      } catch {
        case e =>
          logger.error(s"${e.getStackTraceString}")
          actr ! CreateHiveExternalTableResult(false, e.getMessage)
      }


    case DropHiveExternalTableByChild(tableName, actr) => {
      try {
        HiveTableServiceImpl.deleteTableByName(tableName)
        val tables = HiveTableServiceImpl.listTables()
        if (tables.contains(tableName)) {
          actr ! DropHiveExternalTableResult(false, "")
        }
        else (
          actr ! DropHiveExternalTableResult(true, "")
          )
      } catch {
        case x: Throwable => {
          //          logger.error(s"There has been an exception trying to drop an external table in hive. reason = ${x.getMessage}, stack = \n${x.printStackTrace()}")
          actr ! DropHiveExternalTableResult(false, x.getMessage)
        }
      }

    }
    case ListHiveAllTables => {
      sender ! HiveTableServiceImpl.listTables()
    }

    case BuildTempTableInSparkByChild(org, tableName, hTable, columnInfo, actr) => {
      SqlService.loadTable(org, tableName, hTable, columnInfo)
    }
    case TestSqlRunByChild(sql, actr) => {
      val result = sqlContext.sql(sql).collect()
      logger.info(s"${result.toString}")
    }
    case RunSql(org, sql) => {
      SqlService.runSql(org, sql)
    }
  }



  def save(row: Row, org: String, columns: Array[String], tableName: String, cf: String): Unit = {
    val hbaseClient = new HBaseClient(org)
    val data = row.getValuesMap(columns).asInstanceOf[Map[String, Any]]
    val dataMap = data.map(x => {
      x._2 match {
        case s: String => (x._1, s)
        case i: Int => (x._1, i.toString)
        case d: Double => (x._1, d.toString)
        case l: Long => (x._1, l.toString)
        case _ => (x._1, "")
      }
    })
    val rowKey = UUID.randomUUID().toString
    hbaseClient.insert(tableName, rowKey, cf, dataMap)
  }
}
