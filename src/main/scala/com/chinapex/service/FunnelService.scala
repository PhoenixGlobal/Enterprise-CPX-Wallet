package com.chinapex.service

import java.text.SimpleDateFormat

import com.chinapex.helpers.{ConfigurationHelper, HbaseClientManager}
import com.chinapex.interfaces._
import com.chinapex.utils.SparkHbaseHelper
import org.apache.spark.rdd.RDD
import com.chinapex.helpers.SparkHelper.{sparkContext, sparkSession}
import com.chinapex.legacy.services.{IpResolutionService, UAAResolutionService}
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.util.Bytes
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer

/**
  * Created by sli on 17-9-13.
  */
object FunnelService {

  val logger = LoggerFactory.getLogger(this.getClass)

  def loadData(): Unit = ???

  def getCustomerIDs(fds: List[FilterDetail], org: String): RDD[String] = {
    var distinctCustomers = sparkContext.emptyRDD[String]
    fds.groupBy(_.filterType).foreach(ft => {
      var customerIDs = sparkContext.emptyRDD[String]
      ft._2.foreach(fd => {
        customerIDs = customerIDs ++ getCustomerIDs(org, fd.column, fd.value, fd.value, fd.filterType)
      })
      if (distinctCustomers.isEmpty()) {
        distinctCustomers = distinctCustomers ++ customerIDs.distinct()
      }
      else {
        val pairIDs = customerIDs.map(x => (x, 1))
        distinctCustomers = distinctCustomers.map(x => (x, 1)).join(pairIDs).map(_._1)
      }
    })
    distinctCustomers.distinct
  }

  def getCustomerIDs(org: String, column: String, value: String, id: String, filterType: String): RDD[String] = {
    filterType match {
      case "UserAttributeUA" => {
        getCustomerIDs(org, column, value)
      }
      case "UserAttributeGeo" => {
        getCustomerIDs(org, column, value)
      }
      case "LabelAttribute" => {
        getCustomerIDs(org, id)
      }
      case _ => {
        sparkContext.emptyRDD[String]
      }
    }
  }

  def getCustomerIDs(org: String, column: String, value: String): RDD[String] = {
    val tableName = org + ":" + ConfigurationHelper.prismCustomerEventDataTable
    SparkHbaseHelper.getRDD(tableName, "0", "~~~~", SparkHbaseHelper.res2MapWithK).filter(x => x._2.get(column).fold(false) { v => Bytes.toString(v).equals(value) }).map(_._1)
  }

  def getCustomerIDs(org: String, id: String): RDD[String] = {
    CustomerJourneyService.getCustomerIds(id, org)
  }

  def loadData(org: String, steps: List[FunnelStep], evs: List[(String, String)], st: String, et: String): RDD[EventInfo] = {
    var retRDD = sparkContext.emptyRDD[EventInfo]
    val headStep = steps.filter(_.stepIndex.equals(1)).collectFirst({ case x => x.eventName }).getOrElse("")
    val tableName = org + ":" + ConfigurationHelper.prismEventDataTable
    if (headStep.nonEmpty) {
      val stepEvents = steps.map(_.eventName)
      evs.filter(x => stepEvents.contains(x._2)).foreach(ev => {
        if (ev._2.equals(headStep)) {
          val startRow = s"${org}_${ev._1}_${ev._2}_${st} 0"
          val stopRow = s"${org}_${ev._1}_${ev._2}_${st} ~~~~~~~~"
          retRDD = retRDD ++ SparkHbaseHelper.getRDD(tableName, startRow, stopRow, CustomerJourneyService.res2EventInfo)
        }
        else {
          val startRow = s"${org}_${ev._1}_${ev._2}_${st} 0"
          val stopRow = s"${org}_${ev._1}_${ev._2}_${et} ~~~~~~~~"
          retRDD = retRDD ++ SparkHbaseHelper.getRDD(tableName, startRow, stopRow, CustomerJourneyService.res2EventInfo)
        }
      })
    }
    retRDD
  }

  def calculation(org: String, steps: List[FunnelStep], evs: List[(String, String)], st: String, et: String, filters: List[FilterDetail], id: String, interval: Long): Unit = {
    val hbaseClient = HbaseClientManager(org)
    val existenceCheck = hbaseClient.isTableExist(id)
    if (!existenceCheck) {
      hbaseClient.createTable(id, ConfigurationHelper.funnelResultTableColumnFamily)
    }
    val customerIDs = getCustomerIDs(filters, org)
    val data = loadData(org, steps, evs, st, et)
    var filteredData = sparkContext.emptyRDD[(String, Iterable[EventInfo])]
    if (customerIDs.isEmpty()) {
      filteredData = data.groupBy(_.id)
    }
    else {
      val groupedData = data.groupBy(_.id)
      val pairIDs = customerIDs.map(x => (x, 1))
      filteredData = pairIDs.join(groupedData).map(x => (x._1, x._2._2))
    }
    val stepResult = filteredData.map(x => (x._1, calculationByCustomer(x._2, steps, interval, st)))
    val tailResult = filteredData.map(x => (x._1, tailCalculationByCustomer(x._2, steps, interval, st)))
    val stepFlattened = stepResult.flatMap(x => x._2.map(y => (x._1, y))).zipWithUniqueId().map(x => {
      val step = x._1._2.index
      val k = step + "_" + st + "_" + x._2.toString
      (k, Map[String, Any](
        "customerID" -> x._1._1,
        "step" -> step,
        "date" -> st,
        "interval" -> x._1._2.timing
      ))
    })
    val tableName = org + ":" + id
    SparkHbaseHelper.putRDD(tableName, ConfigurationHelper.funnelResultTableColumnFamily, stepFlattened)
    val stepCount = steps.length
    val tailFlattened = tailResult.flatMap(x => x._2.map(y => (x._1, y))).zipWithUniqueId().filter(x => x._1._2.index.equals(stepCount)).map(x => {
      val step = x._1._2.index
      val k = "tail" + "_" + st + "_" + x._2.toString
      (k, Map[String, Any](
        "customerID" -> x._1._1,
        "step" -> step,
        "date" -> st,
        "interval" -> x._1._2.timing
      ))
    })
    SparkHbaseHelper.putRDD(tableName, ConfigurationHelper.funnelResultTableColumnFamily, tailFlattened)
    logger.warn(s"Done calculation for funnel: $id, org: $org, st: $st, et: $et.")
  }

  def calculationByCustomer(data: Iterable[EventInfo], steps: List[FunnelStep], interval: Long, date: String): List[StepInterval] = {
    val keyLen = steps.length
    val sortedData = data.toList.sortWith(_.ts < _.ts)
    val firstStep = steps.filter(_.stepIndex.equals(1)).collectFirst({ case x: FunnelStep => x.eventName }).getOrElse("")
    if (firstStep.nonEmpty) {
      var index = sortedData.indexWhere(_.en.equals(firstStep))
      val ret = ListBuffer[StepInterval]()
      while (!index.equals(-1)) {
        var stepIndex = 2
        var cursor = index
        val st = sortedData(index).ts
        ret += StepInterval(1, 0)
        while (stepIndex <= keyLen) {
          val step = steps.find(_.stepIndex.equals(stepIndex)).get
          val stepEvent = sortedData.indexWhere(_.en.equals(step.eventName), cursor)
          if (!stepEvent.equals(-1)) {
            cursor = stepEvent + 1
            val et = sortedData(stepEvent).ts
            val timeDiff = endPointCheck(st, et)
            if (timeDiff < interval) {
              ret += StepInterval(stepIndex, timeDiff)
              stepIndex += 1
            }
            else {
              stepIndex = keyLen + 1
            }
          }
          else {
            stepIndex = keyLen + 1
          }
        }
        index = sortedData.indexWhere(_.en.equals(firstStep), index + 1)
        if (ret.filter(_.index.equals(keyLen)).toList.length > 0) {
          index = -1
        }
      }
      ret.toList
    }
    else {
      List()
    }
  }

  def endPointCheck(st: String, et: String): Long = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    sdf.parse(et).getTime - sdf.parse(st).getTime
  }

  def tailCalculationByCustomer(data: Iterable[EventInfo], steps: List[FunnelStep], interval: Long, date: String): List[StepInterval] = {
    val sortedStep = steps.sortWith(_.stepIndex < _.stepIndex)
    val head = sortedStep.head
    val tail = sortedStep.last
    val sortedData = data.toList.sortWith(_.ts < _.ts)
    val ret = ListBuffer[StepInterval]()
    var index = sortedData.indexWhere(_.en.equals(head.eventName))
    while (!index.equals(-1)) {
      val st = sortedData(index).ts
      ret += StepInterval(1, 0)
      val tailIndex = sortedData.indexWhere(_.en.equals(tail.eventName), index)
      if (!tailIndex.equals(-1)) {
        val et = sortedData(tailIndex).ts
        val timeDiff = endPointCheck(st, et)
        if (timeDiff <= interval) {
          ret += StepInterval(tail.stepIndex, timeDiff)
        }
      }
      index = sortedData.indexWhere(_.en.equals(head.eventName), index + 1)
      if (ret.filter(_.index.equals(tail.stepIndex)).toList.length > 0) {
        index = -1
      }
    }
    ret.toList
  }

  def funnelDimensionalCalculation(org: String, id: String, st: String, et: String, stepCount: Int): Unit = {
    val prismCED = org + ":" + ConfigurationHelper.prismEventDataTable
    val ipUa = List("ip", "browser", "customerid")
    val ipUaRDD = SparkHbaseHelper.getRDD(prismCED, "0", "~~~~~", SparkHbaseHelper.res2MapWithK).map(x => (x._1, x._2.filter(y => ipUa.contains(y._1)).map(y => (y._1, Bytes.toString(y._2))))).map(z => (z._2.getOrElse("customerid", ""), z._2))
    val funnelTableName = org + ":" + id
    var funnelRDD = sparkContext.emptyRDD[(String, String)]
    for (i <- 1 to stepCount) {
      val startRow = i.toString + "_" + st + "_" + "0"
      val stopRow = i.toString + "_" + st + "_" + "~~~~"
      val tmpRDD = SparkHbaseHelper.getRDD(funnelTableName, startRow, stopRow, SparkHbaseHelper.res2MapWithK).map(x => (x._1, Bytes.toString(x._2.getOrElse("customerID", Array.empty[Byte]))))
      funnelRDD = funnelRDD ++ tmpRDD
    }
    val tStartRow = "tail" + "_" + st + "_" + "0"
    val tStopRow = "tail" + "_" + st + "_" + "~~~~~~"
    val tailRDD = SparkHbaseHelper.getRDD(funnelTableName, tStartRow, tStopRow, SparkHbaseHelper.res2MapWithK).map(x => (x._1, Bytes.toString(x._2.getOrElse("customerID", Array.empty[Byte]))))
    funnelRDD = funnelRDD ++ tailRDD
    funnelRDD = funnelRDD.map(_.swap)

    val customerIpUa = funnelRDD.join(ipUaRDD)
    val resultRDD = customerIpUa.map(x => {
      val ip = x._2._2.get("ip").getOrElse("")
      val ua = x._2._2.get("browser").getOrElse("")
      val ipResult = IpResolutionService.geoDataFromMem(ip)
      val uaResult = UAAResolutionService.resolve(ua)
      val resultMap = Map(
        "ip" -> ip,
        "city" -> ipResult.city,
        "region" -> ipResult.region,
        "country" -> ipResult.country,
        "browser" -> uaResult.browser,
        "os" -> uaResult.os,
        "device" -> uaResult.device,
        "ua" -> ua
      )
      (x._2._1, resultMap)
    })
    SparkHbaseHelper.putRDD(funnelTableName, ConfigurationHelper.funnelResultTableColumnFamily, resultRDD)
    logger.warn(s"Done dimensional calculation for funnel: $id, org: $org, st: $st, et: $et")
  }

  def stepCount2StepResult(in: List[(Int, List[(String, Int)])], steps: List[FunnelStep]): Map[String, List[StepResult]] = {
    val flattened = in.flatMap(x => x._2.map(y => (y._1, x._1, y._2)))
    val regrouped = flattened.groupBy(_._1)
    val retList = ListBuffer[(String, List[StepResult])]()
    regrouped.foreach(x => {
      val tmpRet = ListBuffer[StepResult]()
      val resorted = x._2.sortWith(_._2 < _._2)
      if (resorted.nonEmpty) {
        val stepResorted = steps.sortWith(_.stepIndex < _.stepIndex)
        val ziped = stepResorted.zipAll(resorted, FunnelStep("", -1), ("", 0, 0))
        var holder = 0
        val t = ziped.map(x => {
          if (x._1.stepIndex.equals(1)) {
            holder = x._2._3
            val rate = {
              if (holder.equals(0)) {
                0.0
              }
              else {
                100.0
              }
            }
            StepResult(x._1.eventName, x._1.stepIndex, "", x._2._3, rate)
          }
          else {
            val rate = {
              if (holder.equals(0)) {
                0.0
              }
              else {
                x._2._3.toDouble * 100 / holder.toDouble
              }
            }
            StepResult(x._1.eventName, x._1.stepIndex, "", x._2._3, rate)
          }
        })
        tmpRet ++= t
      }
      else {
        steps.foreach(s => {
          tmpRet += StepResult(s.eventName, s.stepIndex, "", 0, 0.0)
        })
      }
      retList += Tuple2(x._1, tmpRet.toList)
    })
    retList.toMap
  }

  def stepCount2StepResultWithLoss(in: List[(Int, List[(String, Int)])], steps: List[FunnelStep]): Map[String, List[StepResultWithLoss]] = {
    val flattened = in.flatMap(x => x._2.map(y => (y._1, x._1, y._2)))
    val regrouped = flattened.groupBy(_._1)
    val retList = ListBuffer[(String, List[StepResultWithLoss])]()
    regrouped.foreach(x => {
      val tmpRet = ListBuffer[StepResultWithLoss]()
      val resorted = x._2.sortWith(_._2 < _._2)
      if (resorted.nonEmpty) {
        val stepResorted = steps.sortWith(_.stepIndex < _.stepIndex)
        val ziped = stepResorted.zipAll(resorted, FunnelStep("", -1), ("", 0, 0))
        var holder = 0
        val t = ziped.map(x => {
          if (x._1.stepIndex.equals(1)) {
            holder = x._2._3
            val rate = {
              if (holder.equals(0)) {
                0.0
              }
              else {
                100.0
              }
            }
            StepResultWithLoss(x._1.eventName, x._1.stepIndex, "", x._2._3, 0, rate, 0.0)
          }
          else {
            val rate = {
              if (holder.equals(0)) {
                0.0
              }
              else {
                x._2._3.toDouble * 100 / holder.toDouble
              }
            }
            val loss = {
              if (holder.equals(0)) {
                0
              }
              else {
                holder - x._2._3
              }
            }
            val lossRate = {
              if (holder.equals(0)) {
                0.0
              }
              else {
                loss.toDouble * 100 / holder.toDouble
              }
            }
            StepResultWithLoss(x._1.eventName, x._1.stepIndex, "", x._2._3, loss, rate, lossRate)
          }
        })
        tmpRet ++= t
      }
      else {
        steps.foreach(s => {
          tmpRet += StepResultWithLoss(s.eventName, s.stepIndex, "", 0, 0, 0.0, 0.0)
        })
      }
      retList += Tuple2(x._1, tmpRet.toList)
    })
    retList.toMap
  }

  def funnelGraphCalculatorConversionTime(org: String, id: String, stepCount: Int, st: String, et: String, x: String, filters: List[FilterDetail]): Map[String, ConversionTiming] = {
    val funnelTableName = org + ":" + id
    val startRow = stepCount + "_" + st + "_" + "0"
    val stopRow = stepCount + "_" + et + "_" + "~~~~~~"
    x match {
      case "label" => {
        val rlpName = org + ":" + ConfigurationHelper.reverseLabelPointerTable
        val labelRDD = SparkHbaseHelper.getRDD(rlpName, "0", "~~~~~~", SparkHbaseHelper.res2MapWithK).map(y => (y._1, Bytes.toString(y._2.getOrElse(ConfigurationHelper.reverseLabelPointerQualifier, ";".getBytes())).split(";"))).persist()
        val labelIDs = filters.map(_.value)
        val funnelRDD = SparkHbaseHelper.getRDD(funnelTableName, startRow, stopRow, SparkHbaseHelper.res2Map).map(y => (Bytes.toString(y.getOrElse("customerID", "".getBytes())), Bytes.toLong(y.getOrElse("interval", Array.empty[Byte]))))
        val ret = funnelRDD.join(labelRDD).map(_._2).flatMap(y => y._2.map(z => (z, y._1))).groupBy(_._1).map(y => (y._1, y._2.map(_._2))).collect
        val filtered = {
          if (labelIDs.nonEmpty) {
            ret.filter(y => labelIDs.contains(y._1))
          }
          else {
            ret
          }
        }
        val retList = ListBuffer[(String, ConversionTiming)]()
        filtered.foreach(g => {
          val size = g._2.size
          val reordered = g._2.toList.sortWith(_ < _)
          if (!size.equals(0)) {
            val median = reordered(size / 2)
            val q1 = reordered(size / 4)
            val q3 = reordered(size / 4 * 3)
            val min = reordered.head
            val max = reordered.last
            retList += Tuple2(g._1, ConversionTiming(max, q3, median, q1, min))
          }
          else {
            retList += Tuple2(g._1, ConversionTiming(0L, 0L, 0L, 0L, 0L))
          }
        })
        labelRDD.unpersist(false)
        retList.toMap
      }
      case "date" => {
        val funnelRDD = SparkHbaseHelper.getRDD(funnelTableName, startRow, stopRow, SparkHbaseHelper.res2Map).map(y => (Bytes.toString(y.getOrElse("date", Array.empty[Byte])), Bytes.toLong(y.getOrElse("interval", Array.empty[Byte]))))
        val ret = funnelRDD.groupBy(_._1).map(y => (y._1, y._2.map(_._2))).collect
        val retList = ListBuffer[(String, ConversionTiming)]()
        ret.foreach(g => {
          val size = g._2.size
          if (size.equals(0)) {
            retList += Tuple2(g._1, ConversionTiming(0L, 0L, 0L, 0L, 0L))
          }
          val reordered = g._2.toList.sortWith(_ < _)
          val median = reordered(size / 2)
          val q1 = reordered(size / 4)
          val q3 = reordered(size / 4 * 3)
          val min = reordered.head
          val max = reordered.last
          retList += Tuple2(g._1, ConversionTiming(max, q3, median, q1, min))
        })
        retList.toMap
      }
      case t => {
        val funnelRDD = SparkHbaseHelper.getRDD(funnelTableName, startRow, stopRow, SparkHbaseHelper.res2Map).map(y => (Bytes.toString(y.getOrElse(x, Array.empty[Byte])), Bytes.toLong(y.getOrElse("interval", Array.empty[Byte]))))
        val ret = funnelRDD.groupBy(_._1).map(y => (y._1, y._2.map(_._2))).collect
        val values = filters.map(_.value)
        val retList = ListBuffer[(String, ConversionTiming)]()
        val filtered = {
          if (values.nonEmpty) {
            ret.filter(y => values.contains(y._1))
          }
          else {
            ret
          }
        }
        filtered.foreach(g => {
          filtered.foreach(g => {
            val size = g._2.size
            val reordered = g._2.toList.sortWith(_ < _)
            if (!size.equals(0)) {
              val median = reordered(size / 2)
              val q1 = reordered(size / 4)
              val q3 = reordered(size / 4 * 3)
              val min = reordered.head
              val max = reordered.last
              retList += Tuple2(g._1, ConversionTiming(max, q3, median, q1, min))
            }
            else {
              retList += Tuple2(g._1, ConversionTiming(0L, 0L, 0L, 0L, 0L))
            }
          })
        })
        retList.toMap
      }
    }
  }

  def funnelGraphCalculatorAttributes(org: String, id: String, stepCount: Int, st: String, et: String, x: String, filters: List[FilterDetail]): List[(Int, List[(String, Int)])] = {
    val supportedAttributes = List("city", "region", "country", "os", "browse", "device")
    val filterValues = filters.map(_.value)
    val funnelTableName = org + ":" + id
    val retList = ListBuffer[(Int, List[(String, Int)])]()
    for (i <- 1 to stepCount) {
      val startRow = i.toString + "_" + st + "_" + "0"
      val stopRow = i.toString + "_" + st + "_" + "~~~~~"
      val funnelRDD = SparkHbaseHelper.getRDD(funnelTableName, startRow, stopRow, SparkHbaseHelper.res2Map).map(y => (Bytes.toString(y.getOrElse("customerID", Array.empty[Byte])), Bytes.toString(y.getOrElse(x, Array.empty[Byte])))).distinct
      val ret = funnelRDD.map(_.swap).map(y => (y._1, 1)).reduceByKey(_ + _).collect
      if(filterValues.nonEmpty){
        retList += Tuple2(i, ret.toList.filterNot(_._1.equals("")).filter(z => filterValues.contains(z._1)))
      }
      else{
        retList += Tuple2(i, ret.toList.filterNot(_._1.equals("")))
      }
    }
    retList.toList
  }

  def funnelGraphCalculatorDate(org: String, id: String, stepCount: Int, st: String, et: String): List[(Int, List[(String, Int)])] = {
    val funnelTableName = org + ":" + id
    val retList = ListBuffer[(Int, List[(String, Int)])]()
    for (i <- 1 to stepCount) {
      val startRow = i.toString + "_" + st + "_" + "0"
      val stopRow = i.toString + "_" + et + "_" + "~~~~~"
      val funnelRDD = SparkHbaseHelper.getRDD(funnelTableName, startRow, stopRow, SparkHbaseHelper.res2Map).map(x => (Bytes.toString(x.getOrElse("customerID", Array.empty[Byte])), Bytes.toString(x.getOrElse("date", Array.empty[Byte])))).distinct()
      val ret = funnelRDD.map(_.swap).map(x => (x._1, 1)).reduceByKey(_ + _).collect
      retList += Tuple2(i, ret.toList.filterNot(_._1.equals("")))
    }
    retList.toList
  }

  def funnelGraphCalculatorLabel(org: String, id: String, stepCount: Int, st: String, et: String, filters: List[FilterDetail]): List[(Int, List[(String, Int)])] = {
    val rlpName = org + ":" + ConfigurationHelper.reverseLabelPointerTable
    val funnelTableName = org + ":" + id
    val labelRDD = SparkHbaseHelper.getRDD(rlpName, "0", "~~~~~~", SparkHbaseHelper.res2MapWithK).map(x => (x._1, Bytes.toString(x._2.getOrElse(ConfigurationHelper.reverseLabelPointerQualifier, ";".getBytes())).split(";"))).persist()
    val labelIDs = filters.map(_.value)
    val retList = ListBuffer[(Int, List[(String, Int)])]()
    for (i <- 1 to stepCount) {
      val startRow = i.toString + "_" + st + "_" + "0"
      val stopRow = i.toString + "_" + et + "_" + "~~~~~"
      val funnelRDD = SparkHbaseHelper.getRDD(funnelTableName, startRow, stopRow, SparkHbaseHelper.res2Map).map(x => (Bytes.toString(x.getOrElse("customerID", Array.empty[Byte])), 1)).distinct
      val ret = funnelRDD.join(labelRDD).map(x => x._2).flatMap(x => x._2.map(y => (y, x._1))).reduceByKey(_ + _).collect()
      if (labelIDs.nonEmpty) {
        retList += Tuple2(i, ret.filter(x => labelIDs.contains(x._1)).toList)
      }
      else {
        retList += Tuple2(i, ret.toList)
      }
    }
    labelRDD.unpersist(false)
    retList.toList
  }

  def funnelGraphCalculator(org: String, id: String, stepCount: Int, st: String, et: String, x: String, y: String, filters: List[FilterDetail], steps: List[FunnelStep]): Map[String, Any] = {
    y match {
      case "conversionRate" => {
        x match {
          case "date" => {
            val data = funnelGraphCalculatorDate(org, id, stepCount, st, et)
            stepCount2StepResult(data, steps)
          }
          case "label" => {
            val data = funnelGraphCalculatorLabel(org, id, stepCount, st, et, filters)
            stepCount2StepResult(data, steps)
          }
          case z => {
            val data = funnelGraphCalculatorAttributes(org, id, stepCount, st, et, z, filters)
            stepCount2StepResult(data, steps)
          }
        }
      }
      case "conversionTime" => {
        funnelGraphCalculatorConversionTime(org, id, stepCount, st, et, x, filters)
      }
      case "customerLoss" => {
        x match {
          case "date" => {
            val data = funnelGraphCalculatorDate(org, id, stepCount, st, et)
            stepCount2StepResultWithLoss(data, steps)
          }
          case "label" => {
            val data = funnelGraphCalculatorLabel(org, id, stepCount, st, et, filters)
            stepCount2StepResultWithLoss(data, steps)
          }
          case z => {
            val data = funnelGraphCalculatorAttributes(org, id, stepCount, st, et, z, filters)
            stepCount2StepResultWithLoss(data, steps)
          }
        }
      }
    }
  }

  def calculateOverview(org: String, id: String, stepCount: Int, st: String, et: String): FunnelOverview = {
    val tableName = org + ":" + id
    val headStartRow = "1_" + st + "_0"
    val headStopRow = "1_" + et + "_~~~~"
    val startCount = SparkHbaseHelper.getRDD(tableName, ConfigurationHelper.funnelResultTableColumnFamily, "customerID", headStartRow, headStopRow).distinct.count
    val endStartRow = stepCount.toString + "_" + st + "_0"
    val endStopRow = stepCount.toString + "_" + et + "_~~~~"
    val endCount = SparkHbaseHelper.getRDD(tableName, ConfigurationHelper.funnelResultTableColumnFamily, "customerID", endStartRow, endStopRow).distinct.count
    val diff = startCount - endCount
    val conversionRate = {
      if (startCount.equals(0L)) {
        0.0
      }
      else {
        endCount.toDouble * 100 / startCount.toDouble
      }
    }
    val intervalRDD = SparkHbaseHelper.getRDD(tableName, endStartRow, endStopRow, SparkHbaseHelper.res2Map).map(z => Bytes.toLong(z.getOrElse("interval", Bytes.toBytes(0L))))
    val interval = {
      if(intervalRDD.isEmpty()){
        0L
      }
      else{
        intervalRDD.reduce(_ + _)
      }
    }
    val intervalRet = {
      if (endCount.equals(0L)) {
        0L
      }
      else {
        interval / endCount
      }
    }
    logger.info(s"FunnelOverview\nid = $id, endCount = $endCount, diff = $diff, interval = $interval, conversionRate = $conversionRate")
    FunnelOverview(id, "", endCount.toInt, diff.toInt, intervalRet.toString, conversionRate, 0.0, "", "")
  }

  def timedDetailCalculation(org: String, id: String, stepCount: Int, st: String, et: String, steps: List[FunnelStep]): FunnelDetailResult = {
    var funnelRDD = sparkContext.emptyRDD[(String, Int)]
    val tableName = org + ":" + id
    for (i <- 1 to stepCount) {
      val startRow = i.toString + "_" + st + "_" + "0"
      val stopRow = i.toString + "_" + et + "_" + "~~~~"
      funnelRDD = funnelRDD ++ SparkHbaseHelper.getRDD(tableName, startRow, stopRow, SparkHbaseHelper.res2Map).map(x => (Bytes.toString(x.getOrElse("customerID", Array.empty[Byte])), Bytes.toInt(x.getOrElse("step", Bytes.toBytes(1)))))
    }
    val names = steps.sortWith(_.stepIndex < _.stepIndex).map(_.eventName)
    val stepRes = funnelRDD.distinct.groupBy(_._2).map(x => x._2.map(y => (x._1.toInt, 1))).flatMap(x => x).reduceByKey(_ + _).collect().sortWith(_._1 < _._1).zip(names)
    var lastCount = 0
    val stepResults = stepRes.map(x => {
      if (x._1._1.equals(1)) {
        lastCount = x._1._2
        val rate = {
          if (lastCount.equals(0)) {
            0.0
          }
          else {
            100.0
          }
        }
        StepResult(x._2, x._1._1, "", x._1._2, rate)
      }
      else {
        val rate = {
          if (lastCount.equals(0)) {
            0.0
          }
          else {
            x._1._2.toDouble * 100 / lastCount.toDouble
          }
        }
        lastCount = x._1._2
        StepResult(x._2, x._1._1, "", x._1._2, rate)
      }
    })
    val intervalStartRow = stepCount.toString + "_" + st + "_" + "0"
    val intervalStopRow = stepCount.toString + "_" + et + "_" + "~~~~"
    val intervalRDD = SparkHbaseHelper.getRDD(tableName, intervalStartRow, intervalStopRow, SparkHbaseHelper.res2Map).map(x => Bytes.toLong(x.getOrElse("interval", Bytes.toBytes(0L))))
    val intervalCount = intervalRDD.count()
    val interval = {
      if(intervalCount.equals(0L)){
        0L
      }
      else{
        intervalRDD.reduce(_ + _) / intervalCount
      }
    }
    val tailStartRow = "tail_" + st + "_" + "0"
    val tailStopRow = "tail_" + et + "_" + "~~~~"
    val tailRDD = SparkHbaseHelper.getRDD(tableName, tailStartRow, tailStopRow, (_) => 1L)
    val tailCount = {
      if(tailRDD.isEmpty()){
        0L
      }
      else{
        tailRDD.reduce(_ + _)
      }
    }
    val headStartRow = "1_" + st + "_0"
    val headStopRow = "1_" + et + "_~~~~"
    val headCount = SparkHbaseHelper.getRDD(tableName, headStartRow, headStopRow, (_) => 1L).count
    val tailRate = {
      if(headCount.equals(0L)){
        0.0
      }
      else{
        tailCount.toDouble * 100 / headCount.toDouble
      }
    }
    val endStartRow = stepCount.toString + "_" + st + "_0"
    val endStopRow = stepCount.toString + "_" + et + "_~~~~"
    val endCount = SparkHbaseHelper.getRDD(tableName, endStartRow, endStopRow, (_) => 1L).count
    val conversionRate = {
      if(headCount.equals(0L)){
        0.0
      }
      else{
        endCount.toDouble * 100 / headCount.toDouble
      }
    }
    logger.info(s"FunnelTimedResult\nid = $id, conversionRate = $conversionRate, interval = $interval, tailRate = $tailRate, stepR = ${stepResults.toString}")
    FunnelDetailResult(id, "", conversionRate, 0.0, interval.toString, tailRate, stepResults.toList)
  }

}
