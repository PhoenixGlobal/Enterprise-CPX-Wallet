package com.chinapex

import akka.util.Timeout
import scala.concurrent.duration._
import com.chinapex.helpers.SparkHelper

/**
  *
  */
class TestSpark {
  implicit val timeout: Timeout = 30 seconds

  //init
  val hiveContext = SparkHelper.hiveContext

  def printTables: Unit = {
    val dataFrame = hiveContext.sql("show tables")

    println("=======printTables=======")
    println("show tables - " + dataFrame.show(1000, false))

    val dataFrame2 = hiveContext.sql("select count(*) from laiyifen_prism_event_data")
    println("=======printTable laiyifen_prism_event_data=======")
    println("laiyifen_prism_event_data - " + dataFrame2.show(1000, false))

    val dataFrameLimit2 = hiveContext.sql("select * from laiyifen_prism_event_data limit 2")
    println("=======printTable laiyifen_prism_event_data limit 2=======")
    println("laiyifen_prism_event_data - " + dataFrameLimit2.show(1000, false))
  }
}
