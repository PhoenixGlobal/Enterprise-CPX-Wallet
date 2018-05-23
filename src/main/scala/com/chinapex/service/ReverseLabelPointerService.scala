package com.chinapex.service

import com.chinapex.dao.LabelIDMapDaoObj
import com.chinapex.helpers.{ConfigurationHelper, HbaseClientManager}
import com.chinapex.tools.orm.LabelIDMap
import com.chinapex.utils.SparkHbaseHelper
import org.apache.hadoop.hbase.util.Bytes
import org.slf4j.LoggerFactory

/**
  * Created by sli on 17-9-15.
  */
object ReverseLabelPointerService {

  val logger = LoggerFactory.getLogger(this.getClass)

  def ScheduledUpdate(org: String): Unit = {
    val tableName = org + ":" + ConfigurationHelper.labelCustomerIDHTable
    val rlpName = org + ":" + ConfigurationHelper.reverseLabelPointerTable
    val hbaseClient = HbaseClientManager(org)
    if(!hbaseClient.isTableExist(ConfigurationHelper.reverseLabelPointerTable)){
      hbaseClient.createTable(ConfigurationHelper.reverseLabelPointerTable, ConfigurationHelper.reverseLabelPointerFamily)
    }
    val labelMap = LabelIDMapDaoObj.getAllLabels().fold(List[LabelIDMap]()){x => x}.map(x => (x.id, x.labelID)).toMap
    val labelRDD = SparkHbaseHelper.getRDD(tableName, "0", "~~~~", SparkHbaseHelper.res2MapWithK).map(x => (x._1.split("@")(0), Bytes.toString(x._2.getOrElse(ConfigurationHelper.labelCustomerIDHTableCidColumnQualifier, Array.empty[Byte]))).swap)
    val resultRDD = labelRDD.groupBy(_._1).filter(_._1.nonEmpty).map(x => {
      val k = x._1
      val labelIDs = x._2.map(y => labelMap.getOrElse(y._2.toInt, "")).filterNot(_.equals("")).mkString(";")
      (k, Map(ConfigurationHelper.reverseLabelPointerQualifier -> labelIDs))
    })
    SparkHbaseHelper.putRDD(rlpName, ConfigurationHelper.reverseLabelPointerFamily, resultRDD)
  }
}
