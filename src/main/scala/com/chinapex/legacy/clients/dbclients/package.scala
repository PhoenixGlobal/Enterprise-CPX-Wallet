package com.chinapex.legacy.clients.dbclients

import java.sql.Connection

import com.chinapex.legacy.dtos.ActionResult
import com.chinapex.legacy.util.{DateFormat, TryWith}
import org.slf4j.LoggerFactory

object SqliteClient {
  private val logger = LoggerFactory.getLogger("SQLiteClient")

  def getConnection(): Connection = {
    import com.chinapex.legacy.dao.DataBaseConnectionPool.cpds
    cpds.getConnection
  }

  def apply(): SqliteClientBase = {
    //    var connection: Option[Connection] = None // do not use global var

    //    ConfigurationHelper.isTest match {
    //      case true =>
    //        logger.info("******************** fake sqlite client ***************")
    //        new FakeSqliteClient
    //      case _ =>
    //        new SqliteClient()
    //    }
    new SqliteClient()
  }
}

trait SqliteClientBase {
  def updateData(tableName: String, keyName: String, keyValue: String, updateString: String) : ActionResult

  def insertData(tableName: String, keyName: String, keyValue: String, updateEntity: String): ActionResult

  def getData(tableName: String, keyName: Option[String] = None, keyValue: Option[String] = None, column: Option[String] = None): ActionResult

  def deleteData(tableName: String, keyName: String, keyVal: String): ActionResult

  def getKeysOfTable(tableName: String, keyName: String): ActionResult

}

class SqliteClient() extends SqliteClientBase with TryWith {
  private val logger = LoggerFactory.getLogger(this.getClass)

  //  def createTable(sql: String): ActionResult ={
  //    logger.info(s"create table $sql")
  //    val connection = SqliteClient.getConnection()
  //    val stmt = connection.createStatement()
  //    val result = stmt.executeUpdate(sql)
  //    stmt.close()
  //    connection.close()
  //    if(result == 1){
  //      ActionResult(true)
  //    }else{
  //      ActionResult(false)
  //    }
  //  }

  private def connectAndDoAction(sql: String, isUpdate: Boolean = true, keyName: Option[String] = None): ActionResult = {
    logger.debug(s"Query Table: $sql")
    //    Class.forName("com.mysql.jdbc.Driver")
    //    val connection = DriverManager.getConnection(ConfigurationHelper.mysqlAddress,ConfigurationHelper.mysqlAccount,ConfigurationHelper.mysqlPassword)
    val connection = SqliteClient.getConnection()
    val statement = connection.createStatement()
    tryWith[ActionResult]{
      if(isUpdate) {
        val result = statement.executeUpdate(sql)
        if (result == 1) ActionResult(true) else ActionResult(false, Some("can not get data with sql: " + sql), None)
      } else {
        val data = statement.executeQuery(sql)
        var resultList = List[String]()
        while (data.next()){
          resultList = data.getString(keyName.getOrElse("CONTENT")) :: resultList
        }
        if (resultList.isEmpty) ActionResult(false, Some("Not get any data.")) else ActionResult(true, None, Some(resultList))
      }
    }(e => ActionResult(false, Some(e.getMessage + " " + e.getCause)), {statement.close(); connection.close()})

  }

  /**
    * update table
    * */
  override def updateData(tableName: String, keyName: String, keyValue: String, updateString: String) : ActionResult = {

    // when content is a nested-json string, replace \" to \\"
    val content = updateString.replaceAll("\\\\\"","\\\\\\\\\"")
    val sql = s"UPDATE $tableName SET CONTENT = '$content',TIME = '${DateFormat.getRealTime()}' WHERE $keyName = '$keyValue'"

    connectAndDoAction(sql)
  }

  /**
    * insert into table
    * */
  override def insertData(tableName: String, keyName: String, keyValue: String, updateEntity: String): ActionResult ={
    // when content is a nested-json string, replace \" to \\"
    val content = updateEntity.replaceAll("\\\\\"","\\\\\\\\\"")
    val data = SqliteClient().getData(tableName, Some(keyName), Some(keyValue))
    if(! data.isSuccess){
      val sql = s"INSERT INTO $tableName ($keyName,CONTENT,TIME) VALUES ('$keyValue','$content','${DateFormat.getRealTime()}');"
      connectAndDoAction(sql)
    }else{
      ActionResult(false, failReason = Some("Data already exist."))
    }
  }

  /**
    * get data
    * */
  override def getData(tableName: String, keyName: Option[String] = None, keyValue: Option[String] = None, column: Option[String] = None): ActionResult ={
    var sql = ""
    if (keyValue.isDefined && keyName.isDefined){
      sql = s"SELECT * FROM $tableName WHERE ${keyName.get} = '${keyValue.get}'"
    }else{
      sql = s"SELECT * FROM $tableName"
    }
    connectAndDoAction(sql, false, column)
  }

  override def getKeysOfTable(tableName: String, keyName: String): ActionResult = {
    connectAndDoAction(s"SELECT $keyName FROM $tableName", false, Some(keyName))
  }

  /**
    * delete data from sqlite
    * */
  override def deleteData(tableName: String, keyName: String, keyVal: String): ActionResult = {
    val data = SqliteClient().getData(tableName, Some(keyName), Some(keyVal))
    if(data.isSuccess){
      val sql = s"DELETE FROM $tableName WHERE $keyName = '$keyVal'"
      connectAndDoAction(sql)
    }else{
      ActionResult(false, failReason = Some("Can't find data to delete."))
    }
  }
}
