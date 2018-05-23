package com.chinapex.dataservice

import java.io.File

import com.chinapex.dataservice
import com.typesafe.config.{Config, ConfigFactory, ConfigRenderOptions}

/**
  * load all config file
  */
object DataServiceConfig {
  private val fileConf = ConfigFactory.parseFile(new File("./application.conf"))
  private val online = ConfigFactory.parseResourcesAnySyntax("online")
  private val local = ConfigFactory.parseResourcesAnySyntax("local")
  private val develop = ConfigFactory.parseResourcesAnySyntax("application") //application is also develop environment in this project
  private val default = ConfigFactory.load() //default environment

  private val myConfig = fileConf.withFallback(online).withFallback(local).withFallback(develop)
  private val combinedConfig = myConfig.withFallback(default)

  val akkaConfig = combinedConfig.withOnlyPath("akka")

  val dataServiceConfig = combinedConfig.getConfig("com.chinapex.dataService")
  val dataServiceCf = DataServiceCf(
    dataServiceConfig.getString("hbase.zookeeper.quorum"),
    dataServiceConfig.getString("sparkMaster"),
    dataServiceConfig.getString("localIpData"),
//    {
//      val mysql = dataServiceConfig.getConfig("mysql")
//      MysqlCf(
//        mysql.getString("dataServiceURL"),
//        mysql.getString("platformURL"),
//        mysql.getString("platformUsername"),
//        mysql.getString("platformPassword")
//      )
//    },
    dataServiceConfig.getString("actorSysName")

  )

  def prefixConfig(prefix: String, srcConfig: Config) = ConfigFactory.parseString(prefix).withFallback(srcConfig)
  def printConf(config: Config): Unit = println(config.root().render(ConfigRenderOptions.concise().setFormatted(true).setJson(true)))

  println("===========DataServiceConfigBegin=============")
  printConf(myConfig)
  println("===========DataServiceConfigEnd===============")
}

case class DataServiceCf(hbaseZpr: String,
                         sparkMaster: String,
                         localIpData: String,
//                         mysqlCf: MysqlCf,
                         actorSysName: String
                        )
//
//case class MysqlCf(dataServiceURL: String,
//                   platformURL: String,
//                   platformUsername: String,
//                   platformPassword: String
//                  )