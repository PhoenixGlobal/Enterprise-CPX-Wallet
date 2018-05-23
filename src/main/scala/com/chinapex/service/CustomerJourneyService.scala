package com.chinapex.service

/**
  * Created by sli on 17-9-11.
  */

import akka.actor.ActorRef
import com.chinapex.helpers.SparkHelper.sparkContext
import org.slf4j.LoggerFactory
import com.chinapex.dao.LabelIDMapDaoObj
import com.chinapex.helpers.ConfigurationHelper
import com.chinapex.interfaces.{CustomerJourneyCalculationResult, CustomerJourneyResult, EventInfo}
import com.chinapex.utils.SparkHbaseHelper
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

object CustomerJourneyService {

  val logger = LoggerFactory.getLogger(this.getClass)

  def getCustomerIds(id: String, org: String): RDD[String] = {
    val shortId = LabelIDMapDaoObj.getShortID(id)
    val startRow = shortId.toString + "@" + "0"
    val stopRow = shortId.toString + "@" + "~~~~~"
    val tableName = org + ":" + ConfigurationHelper.labelCustomerIDHTable
    SparkHbaseHelper.getRDD(tableName,
                            ConfigurationHelper.labelCustomerIDHTableCF,
                            ConfigurationHelper.labelCustomerIDHTableCidColumnQualifier,
                            startRow,
                            stopRow)
  }

  def getCustomerIds(ids: List[String], org: String): RDD[String] = {
    var retRdd = sparkContext.emptyRDD[String]
    ids.foreach(id => {
      val shortId = LabelIDMapDaoObj.getShortID(id)
      val startRow = shortId.toString + "@" + "0"
      val stopRow = shortId.toString + "@" + "~~~~~"
      val tableName = org + ":" + ConfigurationHelper.labelCustomerIDHTable
      retRdd = retRdd ++ SparkHbaseHelper.getRDD(
                                                  tableName,
                                                  ConfigurationHelper.labelCustomerIDHTableCF,
                                                  ConfigurationHelper.labelCustomerIDHTableCidColumnQualifier,
                                                  startRow,
                                                  stopRow)
    })
    retRdd.distinct()
  }

  def res2EventInfo(res: Result): EventInfo = {
    val id = Bytes.toString(res.getValue(ConfigurationHelper.prismEventClientInfoFamily.getBytes(), "customerid".getBytes()))
    val eventName = Bytes.toString(res.getValue(ConfigurationHelper.prismEventInfoFamily.getBytes(), "eventname".getBytes()))
    val timestamp = Bytes.toString(res.getValue(ConfigurationHelper.prismEventInfoFamily.getBytes(), "date".getBytes()))
    EventInfo(id, timestamp, eventName)
  }

  def loadData(org: String, id: String, st: String, et: String, evs: List[(String, String)]): RDD[(String, Iterable[EventInfo])] = {
    val retRDD = loadData(org, st, et, evs)
    val idRDD = getCustomerIds(id, org).map(x => (x, 1))
    val idGroupedEI = retRDD.groupBy(_.id)
    idRDD.join(idGroupedEI).map(x => (x._1, x._2._2))
  }

  def loadData(org: String, ids: List[String], st: String, et: String, evs: List[(String, String)]): RDD[(String, Iterable[EventInfo])] = {
    val retRDD = loadData(org, st, et, evs)
    val idRDD = getCustomerIds(ids, org).map(x => (x, 1))
    val idGroupedEI = retRDD.groupBy(_.id)
    idRDD.join(idGroupedEI).map(x => (x._1, x._2._2))
  }

  def loadData(org: String, st: String, et: String, evs: List[(String, String)]): RDD[EventInfo] = {
    var retRDD = sparkContext.emptyRDD[EventInfo]
    val tableName = org + ":" + ConfigurationHelper.prismEventDataTable
    evs.foreach(ep => {
      val startRow = s"${org}_${ep._1}_${ep._2}_${st} 0"
      val stopRow = s"${org}_${ep._1}_${ep._2}_${et} ~~~~~~~~~~"
      retRDD = retRDD ++ SparkHbaseHelper.getRDD(
        tableName,
        startRow,
        stopRow,
        res2EventInfo
      )
    })
    retRDD
  }

  def cleanReorderData(rdd: RDD[(String, Iterable[EventInfo])], r: Boolean = false): RDD[(String, Iterable[String])] = {
    if(r){
      rdd.map(x => {
        val orderedEI = "" :: x._2.toList.sortWith(_.ts > _.ts).map(_.en)
        val eliminatedRepeats = orderedEI.sliding(2).toList.map({
            case List(a, v) => Tuple2(a, v)
            case _ => Tuple2("", "")
        }).filterNot(x => x._1.equals(x._2)).map(_._2)
        (x._1, eliminatedRepeats)
      })
    }
    else{
      rdd.map(x => {
        val orderedEI = "" :: x._2.toList.sortWith(_.ts < _.ts).map(_.en)
        val eliminatedRepeats = orderedEI.sliding(2).toList.map({
            case List(a, v) => Tuple2(a, v)
            case _ => Tuple2("", "")
        }).filterNot(x => x._1.equals(x._2)).map(_._2)
        (x._1, eliminatedRepeats)
      })
    }
  }

  def calculation(org: String, id: String, st: String, et: String, evs: List[(String, String)], actr: ActorRef): Unit = {
    val rawData = loadData(org, id, st, et, evs)
    rawData2Result(org, rawData, id, st, evs, actr)
  }

  def calculation(org: String, ids: List[String], st: String, et: String, evs: List[(String, String)], id: String, actr: ActorRef): Unit = {
    val rawData = loadData(org, ids, st, et, evs)
    rawData2Result(org, rawData, id, st, evs, actr)
  }

  def rawData2Result(org: String, rawData: RDD[(String, Iterable[EventInfo])], id: String, st: String, evs: List[(String, String)], actr: ActorRef): Unit = {
    val leftData = cleanReorderData(rawData, true)
    val rightData = cleanReorderData(rawData)
    evs.foreach(ev => {
      val leftRet = leftData.map(x => calculationByCustomer(x._2, ev._2, true)).flatMap(x => x).groupBy(x => x.parent).map(group => {
        val ret = group._2.map(x => (x.name, x.count)).groupBy(_._1).map(x => (x._1, x._2.map(_._2).reduce(_ + _)))
        val k = st + "_" + "R" + "_" + id + "_" + ev._2 + "_" + group._1
        (k, ret)
      })
      val rightRet = rightData.map(x => calculationByCustomer(x._2, ev._2)).flatMap(x => x).groupBy(x => x.parent).map(group => {
        val ret = group._2.map(x => (x.name, x.count)).groupBy(_._1).map(x => (x._1, x._2.map(_._2).reduce(_ + _)))
        val k = st + "_" + id + "_" + ev._2 + "_" + group._1
        (k, ret)
      })
      val finalResult = leftRet ++ rightRet
      val tableName = org + ":" + ConfigurationHelper.customerJourneyResultTable
      SparkHbaseHelper.putRDD(tableName, ConfigurationHelper.customerJourneyResultTableColumnFamily, finalResult)
      logger.warn(s"org = $org, ev = ${ev._2}, st = $st, id = $id.")
    })
  }

  def calculationByCustomer(data: Iterable[String], ev: String, r: Boolean = false): List[CustomerJourneyResult] = {
    val ret = ListBuffer[CustomerJourneyResult]()
    val customerRet = ListBuffer[CustomerJourneyResult]()
    val listData = data.toList
    var index = listData.indexWhere(_.equals(ev))
    var subData = listData
    while (!index.equals(-1)){
      subData = subData.drop(index)
      val validEvs = subData.take(10).zipWithIndex
      validEvs.foreach(ve => {
        if(ve._2.equals(0)){
          ret += CustomerJourneyResult(ve._1, ve._2, "", 1L)
        }
        else{
          val lastName = validEvs.take(ve._2).map(_._1).mkString("_")
          if(r){
            ret += CustomerJourneyResult(ve._1, -(ve._2), lastName, 1L)
          }
          else{
            ret += CustomerJourneyResult(ve._1, ve._2, lastName, 1L)
          }
        }
      })
      index = subData.indexWhere(_.equals(ev), index + 1)
    }
    val groupedResults = ret.groupBy(x => (x.name, x.index, x.parent))
    groupedResults.foreach(group => {
      val counter = group._2.map(_.count).foldLeft(0L)(_ + _)
      customerRet += CustomerJourneyResult(group._1._1, group._1._2, group._1._3, counter)
    })
    customerRet.toList
  }
}
