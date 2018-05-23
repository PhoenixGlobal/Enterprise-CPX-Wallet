package com.chinapex.helpers

import java.util.concurrent.ConcurrentHashMap

import com.chinapex.tools.dbclients.HBaseClient
import org.apache.commons.lang.StringUtils

import scala.collection.mutable

/**
  * Created by Bin on 04/09/2017.
  */
//object HbaseClientManager {
//  private val hbaseClientOrgMap = new mutable.HashMap[String, HBaseClient]()
//  private val hbaseClientWithoutNameSpace = new HBaseClient(ConfigurationHelper.hbaseIp, ConfigurationHelper.hbasePort.toInt, ConfigurationHelper.hbaseZnode)
//
//  def apply(orgName: String): HBaseClient = {
//    if (orgName == "") apply()
//    else
//      hbaseClientOrgMap.getOrElseUpdate(orgName,
//        new HBaseClient(ConfigurationHelper.hbaseIp,
//          ConfigurationHelper.hbasePort.toInt,
//          ConfigurationHelper.hbaseZnode,
//          orgName))
//  }
//
//  def apply(): HBaseClient = this.hbaseClientWithoutNameSpace
//}


object HbaseClientManager {
  private val hbaseClientOrgMap = new ConcurrentHashMap[String, HBaseClient]()
  private val hbaseClientWithoutNameSpace = new HBaseClient()

  def apply(orgName: String): HBaseClient = {
    if(StringUtils.isBlank(orgName))  return hbaseClientWithoutNameSpace
    if(hbaseClientOrgMap.get(orgName) == null)
      hbaseClientOrgMap.put(orgName,new HBaseClient(orgName))
    hbaseClientOrgMap.get(orgName)
  }

  def apply(): HBaseClient = this.hbaseClientWithoutNameSpace
}