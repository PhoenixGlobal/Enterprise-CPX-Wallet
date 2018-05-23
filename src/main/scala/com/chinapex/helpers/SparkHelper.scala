package com.chinapex.helpers

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.HiveContext


/**
  * Created by shalcanleo on 2017/4/25.
  */
object SparkHelper {
  val APP_NAME = "DataService"

  val sparkSession = SparkSession.builder.appName(APP_NAME).enableHiveSupport.getOrCreate()
  val sparkContext = sparkSession.sparkContext
  val sqlContext = sparkSession.sqlContext
  val hiveContext = new HiveContext(sparkContext)
  sparkContext.setLogLevel("warn")

//  val sparkContext = {
//    println(Thread.currentThread().getContextClassLoader)
//    println(getClass.getClassLoader)
//    println(Thread.currentThread().getContextClassLoader == getClass.getClassLoader)
//    println()
//    println("class aaaa " + Class.forName("org.apache.spark.serializer.KryoSerializer"))
//    println()
//    val sparkConf = new SparkConf().
//      setAppName(APP_NAME).
//      setMaster(DataServiceConfig.dataServiceCf.sparkMaster). //ConfigurationHelper.sparkMaster).
//      set("spark.executor.memory", ConfigurationHelper.sparkExecutorMemory).
////      setJars(Array(SparkContext.jarOfClass(this.getClass).getOrElse(ConfigurationHelper.jarFilePath))).
//      set("spark.serializer", ConfigurationHelper.sparkSerializer).
//      set("spark.kryoserializer.buffer.max", ConfigurationHelper.kryoserializerMaxBuffer).
//      set("hbase.zookeeper.quorum", DataServiceConfig.dataServiceCf.hbaseZpr)
//    new SparkContext(sparkConf)
//
////    SparkSession.builder().appName(APP_NAME).config("spark.serializer","org.apache.spark.serializer.KryoSerializer").enableHiveSupport().getOrCreate().sparkContext
//  }
//
//  def getSparkHiveContext(appName: String) = {
//    val sparkConf = new SparkConf().setAppName(appName).setMaster(ConfigurationHelper.sparkMaster).
//      set("spark.executor.memory", ConfigurationHelper.sparkExecutorMemory).
//      setJars(Array(SparkContext.jarOfClass(this.getClass).getOrElse(ConfigurationHelper.jarFilePath))).
//      set("spark.serializer", ConfigurationHelper.sparkSerializer).
//      set("spark.kryoserializer.buffer.max", ConfigurationHelper.kryoserializerMaxBuffer).
//      set("javax.jdo.option.ConnectionURL", "jdbc:mysql://192.168.1.230:3306/hive?createDatabaseIfNotExist=true").
//      set("javax.jdo.option.ConnectionDriverName", "com.mysql.jdbc.Driver").
//      set("javax.jdo.option.ConnectionUserName", "hive").
//      set("javax.jdo.option.ConnectionPassword", "hive").
//      set("hive.metastore.local","false").
//      set("hbase.zookeeper.property.clientPort", ConfigurationHelper.hbasePort).
//      set("hbase.zookeeper.quorum", ConfigurationHelper.hbaseIp).
//      set("zookeeper.znode.parent", ConfigurationHelper.hbaseZnode)
//    new SparkContext(sparkConf)
//  }
//
//  def getHBaseTableRDDResult(orgName: String = "",
//                             tableName: String,
//                             startRow: String = "",
//                             stopRow: String = ""):RDD[(ImmutableBytesWritable, Result)] = {
//    val tableNameInOrg = if (orgName.nonEmpty) s"$orgName:$tableName" else tableName
//    val hbaseConf = HBaseConfiguration.create()
//
//    hbaseConf.set("hbase.zookeeper.property.clientPort", ConfigurationHelper.hbasePort)
//    hbaseConf.set("hbase.zookeeper.quorum", ConfigurationHelper.hbaseIp)
//    hbaseConf.set("zookeeper.znode.parent", ConfigurationHelper.hbaseZnode)
//    hbaseConf.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
//    hbaseConf.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)
//
//    hbaseConf.set(TableInputFormat.INPUT_TABLE, tableNameInOrg)
//    if (startRow.nonEmpty) hbaseConf.set(TableInputFormat.SCAN_ROW_START, startRow)
//    if (stopRow.nonEmpty) hbaseConf.set(TableInputFormat.SCAN_ROW_STOP, stopRow)
//
//    sparkContext.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat],
//      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
//      classOf[org.apache.hadoop.hbase.client.Result])
//  }

}
