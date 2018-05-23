package com.chinapex.actors


import akka.actor.{ActorRef, ActorSystem, Props}
import com.chinapex.dataservice.DataServiceConfig
import com.chinapex.helpers.ConfigurationHelper
import com.chinapex.rpc.server.RpcServerActor
import com.chinapex.service.MapRPCService

object ActorSys {
  val system = ActorSystem(DataServiceConfig.dataServiceCf.actorSysName, DataServiceConfig.akkaConfig)
  var awsCtrlActor: Option[ActorRef] = None
  var supporterHolder: Option[ActorRef] = None

  var labelCalActor = system.actorOf(Props[HiveContextActor],"labelCalActor")
  // val labelRecActor = system.actorSelection(ConfigurationHelper.labelRecActorAddr)
  val rpcActor: ActorRef = system.actorOf(
    Props.create(classOf[RpcServerActor], MapRPCService.mapRPCService), "rpcServer")


  def actorOf(): Unit = {
    supporterHolder = Some(system.actorOf(Props[SupporterHolder], "holder"))
  }

  def systemTerminate(): Unit ={
    system.terminate()
  }
}
