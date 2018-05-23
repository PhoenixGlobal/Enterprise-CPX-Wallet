package com.chinapex.service

import com.chinapex.actors.ActorSys
import com.chinapex.dao.LabelIDMapDaoObj
import com.chinapex.helpers.{ConfigurationHelper, HbaseClientManager}
import com.chinapex.interfaces.LabelCoverScopeResponseBySpark
import org.apache.spark.sql._
import org.slf4j.LoggerFactory
import org.apache.spark.sql.hive.HiveContext
import com.chinapex.tools.dbclients.HBaseClient
import scala.collection.mutable

object LabelService {
  val logger = LoggerFactory.getLogger(this.getClass)

  // [orgName, [customerId, masterTable rowKey]]
  val cidMtrkMap: mutable.HashMap[String, Map[String, String]] = new mutable.HashMap[String, Map[String, String]]()

  // (customerId, masterTable rowKey)
  def getCIDAndMasterTableRKMap(orgName: String): Map[String, String] = {
    cidMtrkMap.get(orgName).fold[Map[String, String]]{

      val hbaseClient = HbaseClientManager(orgName)

      val table = ConfigurationHelper.masterTableName
      val family = ConfigurationHelper.masterTableColumnFamily
      val column = ConfigurationHelper.masterCustomerDataId

      logger.info(s"DataService-LabelService-getCIDAndMasterTableRKMap(): Begin scan customerID/MTRK pairs for org: $orgName")
      val tbInfo: Map[String, String] = hbaseClient.getCells(table, family, column).map(_.swap)
      logger.info(s"DataService-LabelService-getCIDAndMasterTableRKMap(): End: ${tbInfo.size} pairs added found.")

      // save
      cidMtrkMap.put(orgName, tbInfo)
      tbInfo
    } (f => f)
  }

  // return: [customerId, masterTable rowKey]
  def combineCIDWithMTRK(cids: List[String], orgName: String): Map[String, String] = {
    val cidMTRKmap: Map[String, String] = getCIDAndMasterTableRKMap(orgName)
    cids.map(cid => cid -> cidMTRKmap.getOrElse(cid, "")).toMap
  }

  // If error happens, will send "0" back to Platform
  def calculateLabelCoverScope(orgName: String, labelID: String, sqls: List[String], hiveContext: SQLContext): Option[LabelCoverScopeResponseBySpark] = {
    logger.info(s"DataService-LabelService-calculateLabelCoverScope() receive job: ($orgName, $labelID), SQLs: $sqls")
    var coverScope = 0
    var customerIDs: List[String] = List.empty[String]
    var res: Map[String, String] = Map.empty

    try {
      val dataFrames: List[DataFrame] = sqls.map( sql => hiveContext.sql(sql) )

      // intersect
      val dataFramesIntersected: DataFrame = dataFrames.reduceLeft((a: DataFrame, b: DataFrame) => {
        a.intersect(b)
      })

      // retrieve coverScope
      coverScope = dataFramesIntersected.count().asInstanceOf[Int]
      // retrieve customer IDs
      customerIDs = dataFramesIntersected.collect().map(_.getString(0)).toList
      // convert customer IDs to Map of [cid, masterTable RowKey]
      res = combineCIDWithMTRK(customerIDs, orgName)

      logger.info(s"HiveContextActor-calculateLabelCoverScope(): coverScope for LabelID: $labelID is: $coverScope")


      // 1. store customer id into HBase "labelCustomerIDHTable"
      // 2. send coverScope result back to Platform

      // 1
      LabelIDMapDaoObj.getShortID(labelID).fold[Unit] ( logger.error(s"Cannot find shortID of label: $labelID") ) ((id: Int) => {
        if (coverScope > 0) {
          val hbaseClient = HbaseClientManager(orgName)
          var idx: Int = 0  // customer id index, start from 1

          // rowkey -> Map(column -> value)
          // rowkey format: shortID@customerID_index
          val data: Map[String, Map[String, String]] = res.map(kv  => {
            idx += 1

            s"${id.toString}@${idx.toString}" ->
              Map(ConfigurationHelper.labelCustomerIDHTableCidColumnQualifier -> kv._1,
                ConfigurationHelper.labelCustomerIDHTableMTRKColumnQualifier -> kv._2)
          })

          hbaseClient.batchInsert(ConfigurationHelper.labelCustomerIDHTable, ConfigurationHelper.labelCustomerIDHTableCF, data)
          logger.info(s"HiveContextActor-calculateLabelCoverScope($orgName, $labelID): customer id inserted successfully!")
        }
      })

      Some( LabelCoverScopeResponseBySpark(orgName, labelID, coverScope) )

    } catch {
      case e =>
        logger.error(s"HiveContextActor-calculateLabelCoverScope(): Exception happened when execute sql for labelID: $labelID")
        logger.error(s"${e.getStackTraceString}")
        None
    }
  }
}
