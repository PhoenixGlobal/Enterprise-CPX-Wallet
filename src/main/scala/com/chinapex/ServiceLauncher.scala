package com.chinapex

import java.io.File
import java.util.concurrent.atomic.AtomicInteger

import com.chinapex.actors.ActorSys
import com.chinapex.exception.RpcException
import com.chinapex.helpers.{ConfigurationHelper, SparkHelper}
import com.chinapex.service.HelloWorldService.TestMsg
import com.chinapex.service.{HelloWorldService, HiveTableService}
import com.chinapex.service.impl.HiveTableServiceImpl

/**
  * Created by shalcanleo on 2017/5/2.
  */
trait ServiceLauncher {
  def startServer(): Unit = {
    // load property 应该放最前面
    ConfigurationHelper.beginLoadProperties(getRootPath)
    println("server start....")
    println("***********************************")
    println(s"* hbaseIp    =   ${ConfigurationHelper.hbaseIp}")
    println(s"* hbasePort  =   ${ConfigurationHelper.hbasePort}")
    println(s"* hbaseZnode =   ${ConfigurationHelper.hbaseZnode}")
    println("***********************************")

    ActorSys.actorOf()

    startRpcServer()
//    ReverseLabelPointerService.ScheduledUpdate("chinapex")

    timertask.start()
//    AwsCtrlService.startTestQuartz()
//    if (ConfigurationHelper.awsTrigger) AwsCtrlService.startRunScheduleInstance(ConfigurationHelper.awsTestInstanceId)
  }

  def startRpcServer(): Unit = {
    val services = Map(
      classOf[HiveTableService].getName -> HiveTableServiceImpl.build(SparkHelper.sparkContext.hadoopConfiguration)
    )
//    val rpcServer = ActorSys.system.actorOf(Props.create(classOf[RpcServerActor],services),"rpcServer")
  }

  def stopServer(): Unit ={
    ActorSys.systemTerminate()
  }

  private def getRootPath: String = {
    try {
      new File(this.getClass.getResource("").getFile).
        getCanonicalPath.split("file:").last.split(".jar").head.split("/").
        reverse.tail.reverse.reduce((a, b) => a + "/" + b)
    }catch{
      case e: Exception =>
        return ""
    }
  }

}
