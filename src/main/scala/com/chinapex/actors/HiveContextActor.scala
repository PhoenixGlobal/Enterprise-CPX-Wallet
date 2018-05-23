package com.chinapex.actors

import akka.actor.Actor

import com.chinapex.dao.LabelIDMapDaoObj
import com.chinapex.helpers.{ConfigurationHelper, HbaseClientManager, MessageClassLoader}
import com.chinapex.helpers.SparkHelper.sparkContext
import com.chinapex.interfaces.{LabelCoverScopeRequestBySpark, LabelCoverScopeResponseBySpark}
import com.chinapex.messages.remote._
import com.chinapex.service.LabelService
import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory

import com.chinapex.helpers.MessageClassLoader
import com.chinapex.messages.remote._
import org.apache.spark.sql.SQLContext


/**
  * Created by pengmingguo on 7/17/17.
  */
class HiveContextActor extends Actor {

 import com.chinapex.helpers.SparkHelper.sparkSession.sql
 import com.chinapex.helpers.SparkHelper.sqlContext

  override def receive = {
    case req: GenericSQLRequest => executeStatement(req)
    case BusinessLogic(mainClazz, auxClazz) => executeBusinessLogic(mainClazz, auxClazz)

    case LabelCoverScopeRequestBySpark(orgName, labelID, sqls) =>
      val res: Option[LabelCoverScopeResponseBySpark]= LabelService.calculateLabelCoverScope(orgName, labelID, sqls, sqlContext)

      // 2. send coverScope result back to Platform
      if (res.isDefined)
        sender ! res.get
      else
        sender ! LabelCoverScopeResponseBySpark(orgName, labelID, 0)
  }

  def executeStatement(req: GenericSQLRequest): Unit = {
    val dataFrame = sql(req.statement)
    req match {
      case SQLRequest(_) => sender ! dataFrame.collect()
      case SingleColumnSQLRequest(_) => sender ! dataFrame.collect().map(row => row.get(0))
      case MultiColumnSQLRequest(_) =>
        val cols = dataFrame.columns
        sender ! dataFrame.collect().map(row => row.getValuesMap(cols))
    }
  }

  def executeBusinessLogic(mainClazz: Array[Byte], auxClazz: List[Array[Byte]]): Unit = {
    val classLoader = new MessageClassLoader
    auxClazz.foreach(bytes => classLoader.defineClass(bytes))
    val clazz = classLoader.defineClass(mainClazz)
    println(clazz)
    val instance = clazz.newInstance().asInstanceOf[SQLContext => Any]
    val result = instance.apply(sqlContext)
    sender() ! result
  }
}

case object GetHiveContext