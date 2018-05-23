package com.chinapex.service.impl

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.conf
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hadoop.hive.metastore.MetaStoreUtils
import org.apache.hive.service.server.HiveServer2
import org.slf4j.LoggerFactory

/**
  * embedded hive server
  * 类的参数为包含　hive-site.xml 的　config
  * 结合spark的话可以通过　sc.hadoopConfiguration获得
  * created by pengmingguo on 8/28/17
  */
class HiveLocalEmbeddedServer2(hadoopConfig: Configuration) {
  private val logger = LoggerFactory.getLogger(this.getClass)

  logger.info("Starting Hive Local/Embedded Server...")
  // 复制一份config
  val hiveConf = new conf.HiveConf(hadoopConfig, classOf[HiveConf])
  val port = MetaStoreUtils.findFreePort()
  hiveConf.setIntVar(ConfVars.HIVE_SERVER2_THRIFT_PORT, port)
  hiveConf.setBoolean("hive.metastore.schema.verification",false)
  // disable web ui
  hiveConf.set("hive.server2.webui.port","-1")
  val hiveServer = new HiveServer2()
  hiveServer.init(hiveConf)
  hiveServer.start()
}
