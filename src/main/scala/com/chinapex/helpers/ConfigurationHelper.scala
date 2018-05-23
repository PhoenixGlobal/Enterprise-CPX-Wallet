package com.chinapex.helpers

import com.amazonaws.regions.Regions
import java.io.{File, FileInputStream}
import java.util.Properties

/**
  * Created by shalcanleo on 2017/4/24.
  */
object ConfigurationHelper {
  var hbaseIp: String = "192.168.3.2"
  var hbasePort: String = "2181"
  var hbaseZnode: String = "/hbase"
  var realtimeDataTable: String = "prism_realtime_data_test"
  var sparkMaster = "spark://192.168.1.199:7077"
  var sparkExecutorMemory = "2g"
  var sparkDriverMemory = "3g"
  var sparkExecutorCores = "2"
  var sparkDriverCores = "3"
  var sparkTaskCpus= "2"
  var sparkCoreMax= "3"
  var sparkSerializer = "org.apache.spark.serializer.KryoSerializer"
  var kryoserializerMaxBuffer = "1024M"
  var awsAccessId = "AKIAOHNO4OELJM2JFCUA"
  var awsAccessKey = "NsKstxI216S3pfvpwfzC39rrPtIshj4O6otUFnBb"
  var awsRegion:Regions = null ; //Regions.CN_NORTH_1
  var awsTrigger= true
  var awsTestInstanceId = "i-0f3857fab07cbc6d4"
  // var labelRecActorAddr = "akka.tcp://labelCalculatingSystem@192.168.1.230:10083/user/labelRecActor"

  var mysqlAddress = "jdbc:mysql://192.168.1.230:3306/CDP_NING?useSSL=false"
  var mysqlAccount = "root"
  var mysqlPassword = ""

  val masterTableName = "masterTable"
  val masterTableColumnFamily = "cf"
  val masterCustomerDataId = "customerId"

  // 用于存储标签对应customer id 数据的HBase table, 代替此前的MySQL "LabelCustomerID" table
  var labelCustomerIDHTable = "labelCustomerIDHTable"
  var labelCustomerIDHTableCF = "cf"
  var labelCustomerIDHTableCidColumnQualifier = "cid"   // column name used to store customer id
  var labelCustomerIDHTableMTRKColumnQualifier = "mtrk" // column name used to store master table row key
  // 维护标签自增ID 和原始标签ID 的MySQL 表
  var labelShortIDTable = "LabelIDMap"
  var prismEventDataTable = "prism_event_data"
  var prismEventInfoFamily = "event_info"
  var prismEventClientInfoFamily = "client_info"

  var prismCustomerEventDataTable = "prism_customer_event_data"
  var prismCustomerEventDataColumnFamily = "cf"

  var funnelResultTableColumnFamily = "cf"

  var customerJourneyResultTable = "CustomerJourneyResult"
  var customerJourneyResultTableColumnFamily = "cf"

  var reverseLabelPointerTable = "reverseLabelPointer"
  var reverseLabelPointerFamily = "cf"
  var reverseLabelPointerQualifier = "label"




//  var labelRecActorAddr = "akka.tcp://labelCalculatingSystem@192.168.1.230:10083/user/labelRecActor"

  var sqlRemoteActorAddr = "akka.tcp://labelCalculatingSystem@192.168.1.230:10083/user/localSqlSupportActor"


  var sqlCreatedTableColumnFamily = "cf"

  def beginLoadProperties(rootPath: String): Unit = {
    loadConfigerations(loadProperties(rootPath))
  }


  private def loadProperties(rootPath: String): Properties = {
    val properties: Properties = new Properties()
    val file = new File(rootPath + "/config.properties")
    if (file.exists()){
      properties.load(new FileInputStream(rootPath + "/config.properties"))
    }
    properties
  }

  private def loadConfigerations(properties: Properties): Unit = {
    hbaseIp = properties.getProperty("hbaseIp", "192.168.3.2")
    hbasePort = properties.getProperty("hbasePort", "2181")
    hbaseZnode = properties.getProperty("hbaseZnode", "/hbase")
    realtimeDataTable = properties.getProperty("realtimeDataTable","prism_realtime_data_test")
    sparkMaster = properties.getProperty("sparkMaster",sparkMaster )
    sparkExecutorMemory = properties.getProperty("sparkExecutorMemory","1g")
    sparkDriverMemory= properties.getProperty("sparkDriverMemory","1g")

    sparkExecutorCores= properties.getProperty("sparkExecutorCores","1")
    sparkDriverCores  = properties.getProperty("sparkDriverCores","1")
    sparkTaskCpus     = properties.getProperty("sparkTaskCpus","1")
    sparkCoreMax      = properties.getProperty("sparkCoreMax","1")

    sparkSerializer = properties.getProperty("sparkSerializer","org.apache.spark.serializer.KryoSerializer")
    kryoserializerMaxBuffer = properties.getProperty("kryoserializerMaxBuffer","1024M")

    if (properties.getProperty("awsAccessId")!=null)
      awsAccessId =  properties.getProperty("awsAccessId")
    if (properties.getProperty("awsAccessKey")!=null)
      awsAccessKey = properties.getProperty("awsAccessKey")
    if (properties.getProperty("awsTrigger")!=null)
      awsTrigger = properties.getProperty("awsTrigger").toBoolean
    if (properties.getProperty("awsRegion")!=null)
      awsRegion = Regions.fromName(properties.getProperty("awsRegion"))
    if (properties.getProperty("awsTestInstanceId")!=null)
      awsTestInstanceId = properties.getProperty("awsTestInstanceId")
//    if (properties.getProperty("labelRecActorAddr")!=null)
//      labelRecActorAddr = properties.getProperty("labelRecActorAddr")

    mysqlAddress = properties.getProperty("mysqlAddress", mysqlAddress)
    mysqlAccount = properties.getProperty("mysqlAccount", mysqlAccount)
    mysqlPassword = properties.getProperty("mysqlPassword", mysqlPassword)
    prismEventDataTable = properties.getProperty("prismEventDataTable", prismEventDataTable)
    prismEventInfoFamily = properties.getProperty("prismEventInfoFamily", prismEventInfoFamily)
    prismEventClientInfoFamily = properties.getProperty("prismEventClientInfoFamily", prismEventClientInfoFamily)
    prismCustomerEventDataTable = properties.getProperty("prismCustomerEventDataTable", prismCustomerEventDataTable)
    funnelResultTableColumnFamily = properties.getProperty("funnelResultTableColumnFamily", funnelResultTableColumnFamily)
    customerJourneyResultTable = properties.getProperty("customerJourneyResultTable", customerJourneyResultTable)
    customerJourneyResultTableColumnFamily = properties.getProperty("customerJourneyResultTableColumnFamily", customerJourneyResultTableColumnFamily)
  }

}
