package com.chinapex

import java.util.Properties

import com.chinapex.dataservice.DataServiceConfig
import com.chinapex.template.JdbcTemplate
import net.liftweb.json.DefaultFormats

/**
  * database connector
  */
package object dao {
  implicit val formats = DefaultFormats

  //move to PublicTools module
//  val dataServiceDb = {
//    val dataServiceProp = new Properties()
//    dataServiceProp.setProperty("druid.url", DataServiceConfig.dataServiceCf.mysqlCf.dataServiceURL)
//    new JdbcTemplate().build(dataServiceProp)
//  }
//
//  val platformDb = {
//    val platformProp = new Properties()
//    platformProp.setProperty("druid.url", DataServiceConfig.dataServiceCf.mysqlCf.platformURL)
//    platformProp.setProperty("druid.username", DataServiceConfig.dataServiceCf.mysqlCf.platformUsername)
//    platformProp.setProperty("druid.password", DataServiceConfig.dataServiceCf.mysqlCf.platformPassword)
//    new JdbcTemplate().build(platformProp)
//  }

}
