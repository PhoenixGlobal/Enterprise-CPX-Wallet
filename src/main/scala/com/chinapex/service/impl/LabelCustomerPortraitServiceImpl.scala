package com.chinapex.service.impl

import com.chinapex.LabelCustomerPoortraitService.dto.{ListLableIdCidRes, MapLableIdCidRes}
import com.chinapex.helpers.{ConfigurationHelper, SparkHelper}
import com.chinapex.helpers.SparkHelper.sparkContext
import com.chinapex.interfaces.LabelCustomerPortraitService
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.junit.Test
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

case class NotImplementException() extends Exception{}

class LabelCustomerPortraitServiceImpl extends  LabelCustomerPortraitService{
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val hiveContext = new HiveContext(sparkContext)

  override def  listLabelIdCid(orgName:String, cid:String) :ListLableIdCidRes={
    val tableName   =  ConfigurationHelper.labelCustomerIDHTable
    val familyName  =  ConfigurationHelper.labelCustomerIDHTableCF
    val tagetColumn =  ConfigurationHelper.labelCustomerIDHTableCidColumnQualifier
    val hiveTableName = s"${orgName}_$tableName"
    //TODO limit counts or do paging
    logger.info(s"LabelCustomerPortraitServiceImpl.listLabelIdCid.start")
    val SQL_STRING = s"""select `rowkey` from $hiveTableName where `cid`="$cid" """
    val dataFrame:DataFrame  = hiveContext.sql(SQL_STRING)
//    val dataFrame:DataFrame  = SparkHelper.sqlContext.sql(SQL_STRING)
    //todo info to debug
    val res = dataFrame.collectAsList().map(_.mkString)
    val listLableIdCidRes = new ListLableIdCidRes
    listLableIdCidRes.setLableIdCidRes(res.asJava)
    logger.debug(s"LabelCustomerPortraitServiceImpl.listLabelIdCid:${listLableIdCidRes.getLableIdCidRes}")
    listLableIdCidRes
  }

  @deprecated
  override def mapLableIdCid(): MapLableIdCidRes = {
    //TODO NotImplement
    throw NotImplementException()
    new MapLableIdCidRes
  }

  private def listOrgFromDao():List[String]={
    List[String]()
  }

  @Test
  def testListLableIdCid():Unit= listLabelIdCid("chinapex","""3c884ed5-1619-441e-aa89-a31990d8b486""")
}
