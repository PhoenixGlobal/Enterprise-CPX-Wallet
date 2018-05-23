package com.chinapex.service.impl

import javax.sql.DataSource

import com.alibaba.druid.pool.DruidDataSource
import com.alibaba.druid.util.JdbcUtils._
import com.chinapex.service.HiveTableService
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.conf.HiveConf
import org.slf4j.LoggerFactory
import scala.collection.JavaConversions._

/**
  * created by pengmingguo on 8/28/17
  */
object HiveTableServiceImpl extends HiveTableService {
  private val logger = LoggerFactory.getLogger(this.getClass)
  private var hiveConf: HiveConf = _
  private var hiveServer: HiveLocalEmbeddedServer2 = _
  private var dataSource: DataSource = _

  def build(conf: Configuration): HiveTableService = {
    this.hiveConf = new HiveConf(conf, classOf[HiveConf])
    this.hiveServer = new HiveLocalEmbeddedServer2(conf)
    val druid = new DruidDataSource()
    druid.setMaxActive(3)
    druid.setTestWhileIdle(false)
    druid.setDriverClassName("org.apache.hive.jdbc.HiveDriver")
    druid.setMinIdle(2)
    druid.setUrl(s"jdbc:hive2://localhost:${hiveServer.port}/default")
    druid.setUsername("")
    druid.setPassword("")
    this.dataSource = druid
    this
  }

  private def getConnection = dataSource.getConnection

  override def listTables(): List[String] = listTable("show tables")

  override def listTables(pattern: String): List[String] =
    listTable(s"show tables like '${pattern}'")

  override def createTable(s: String) = executeSql(s)

  override def deleteTableByName(s: String) = executeSql(s"drop table ${s}")

  override def deleteTable(s: String) = executeSql(s)

  override def updateTable(s: String) = executeSql(s)

  override def doInTransaction[T](
      transactionLogic: HiveTableService.TransactionLogic[T]) = {
    val conn = getConnection
    try {
      conn.setAutoCommit(false)
      val res = transactionLogic.exec()
      conn.commit()
      res
    } catch {
      case e: Throwable =>
        conn.rollback()
        throw e
    } finally {
      conn.setAutoCommit(true)
      close(conn)
    }
  }

  private def executeSql(sql: String) = {
    logger.info(s"execute sql = ${sql}")
    execute(dataSource, sql)
  }

  private def listTable(sql: String) = {
    executeQuery(dataSource, sql)
      .map(m => m.get("tab_name").asInstanceOf[String])
      .toList
  }
}
