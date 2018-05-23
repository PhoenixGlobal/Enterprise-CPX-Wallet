package com.chinapex.script
import com.chinapex.helpers.SparkHelper.sparkContext
import com.chinapex.service.impl.HiveTableServiceImpl
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.hive.HiveContext
import org.junit.Test
import org.slf4j.{Logger, LoggerFactory}

class ScriptHiveCreateTable{

  val APP_NAME = "ScriptHiveCreateTable"
  val SPARK_URL = "spark://192.168.1.199:7077"
  val DEFAULTS_CONF_PATH = "/home/russ/IdeaProjects/CDP/DataService/src/test/resources/spark-defaults.conf"
  val TABLE_ORG_NAME = "chinapex"
  val TABLE_NAME = "labelCustomerIDHTable"
  val TABLE_INFO = List(("cid","String","cf"))

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  val hiveContext = new HiveContext(sparkContext)

  /**
    *from bin's code in HiveExtService
  'info': (columnName, dataType, column_family)
    */
  def createExternalTableInHive(info: List[(String, String, String)], hbaseTableName: String, hiveTableName: String): Unit= {
    logger.info(s"ScriptHiveCreateTable.hiveContext.confproperties:${hiveContext.getAllConfs.toString}")
      val columnWithType = info.map(x => {
        val newType = x._2 match {
          case "String" => "STRING"
          case "Num" | "Int" => "INT"
          case "DOUBLE" => "DOUBLE"
          case "DATETIME" => "TIMESTAMP"
          case _ => "STRING"
        }
        s"`${x._1}` ${newType}"

      }).mkString(",")
      val cfWithQualifier = info.map(x => s"${x._3}:${x._1}").mkString(",")
      val CREATETABLESTATEMENT =
        s"""
           | CREATE EXTERNAL TABLE IF NOT EXISTS $hiveTableName(rowkey string, $columnWithType)
           | STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
           | WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,$cfWithQualifier")
           | TBLPROPERTIES ("hbase.table.name"="$hbaseTableName")
         """.stripMargin

      logger.info(s"HiveExtService.createExternalTableInHive2() sql: $CREATETABLESTATEMENT")


    val conf = new Configuration()
    conf.set("javax.jdo.option.ConnectionURL","jdbc:mysql://192.168.1.230:3306/hive?createDatabaseIfNotExist=true")
    conf.set("javax.jdo.option.ConnectionUserName","hive")
    conf.set("javax.jdo.option.ConnectionPassword","hive")
    HiveTableServiceImpl.build(conf).createTable(CREATETABLESTATEMENT)
    hiveContext.sql(s"describe $hiveTableName").show()
//    println(HiveTableServiceImpl.build(conf).listTables())
  }

  @Test
  def testCreateHiveTable():Unit={
    val hbaseTableName = s"$TABLE_ORG_NAME:$TABLE_NAME"
    val hiveTableName = s"${TABLE_ORG_NAME}_$TABLE_NAME"
    createExternalTableInHive(TABLE_INFO , hbaseTableName, hiveTableName)
  }
}
