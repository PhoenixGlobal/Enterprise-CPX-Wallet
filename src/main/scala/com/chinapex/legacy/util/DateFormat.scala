package com.chinapex.legacy.util

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Locale, TimeZone}

import scala.collection.SortedMap

/**
  * Created by arthur on 16-11-3.
  */
object DateFormat {
  def getRealTime(now : Date = new Date()):String={
    val  dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val getTime = dateFormat.format( now )
    getTime
  }

  def getMinimumDate: String = {
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val getTime = dateFormat.format( new Date(Long.MinValue) )
    getTime
  }

  def mkTime(realTime: String=getRealTime()):Long = {
    val loc = new Locale("en")
    val unixTime = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss",loc).parse(realTime)
    unixTime.getTime
  }

 def mkTimeJsFormat(realTime: String=getRealTime()):Long = {
    val loc = new Locale("en")
    val unixTime = new java.text.SimpleDateFormat("yyyy/MM/dd",loc).parse(realTime)
    unixTime.getTime
  }

    /**
    * fake data -> for generating a SortMap type DailyWeeklyTraffic data
    * */
  def initDailyWeeklyTraffic(n: Int, interval: Int = -1): SortedMap[String,String] = {
    var res : SortedMap[String,String]= SortedMap()
    val date = Calendar.getInstance()
    val minuteFormat = new SimpleDateFormat("yyyy-MM-dd")
    if (n >= 1) {
      for (_ <- 1 to n) {
        res += (minuteFormat.format(date.getTime) -> "0")
        date.add(Calendar.DATE, interval)
      }
    }
    res
  }

  /**
    * fake data -> for generating a SortMap type DailyWeeklyTraffic data
    * */
  def initDailyWeeklyTrafficUTC(n: Int, interval: Int = -1): SortedMap[String,String] = {
    var res : SortedMap[String,String]= SortedMap()
    val date = Calendar.getInstance()
    date.setTimeZone(TimeZone.getTimeZone("UTC"))
    val minuteFormat = new SimpleDateFormat("yyyy-MM-dd")
    minuteFormat.setTimeZone(TimeZone.getTimeZone("UTC"))
    if (n >= 1) {
      for (_ <- 1 to n) {
        res += (minuteFormat.format(date.getTime) -> "0")
        date.add(Calendar.DATE, interval)
      }
    }
    res
  }

}