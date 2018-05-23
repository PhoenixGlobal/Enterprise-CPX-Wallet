package com.chinapex.actors

import java.util.Date

import akka.actor.Actor
import com.typesafe.akka.extension.quartz.QuartzSchedulerExtension

/**
  * Created by shalcanleo on 2017/1/6.
  */
trait ActorWithScheduler extends Actor{
  this: Actor =>
  implicit val postfixOps = language.postfixOps

  def withScheduler(duration: Int)(f: => Unit){
    import context.dispatcher

    import scala.concurrent.duration._

    context.system.scheduler.scheduleOnce(duration milliseconds) {
      f
    }
  }
}
