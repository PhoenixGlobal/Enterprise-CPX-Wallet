package com.chinapex.sparksql

import com.chinapex.helpers.{ConfigurationHelper, SparkHelper}
import com.chinapex.legacy.services.IpResolutionService
import SparkHelper.sparkSession.implicits._
import com.chinapex.dao.{DataResourceDao, DataResourceEntity, UserOrganizationDao}
import com.chinapex.legacy.dao.LangLatDao
import com.chinapex.service.ChartSchemaResponse
import com.chinapex.service.impl.HiveTableServiceImpl
import net.liftweb.json._
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}
/**
  *
  */
object DashboardSpark {
  private implicit val formats = DefaultFormats
  private val logger = LoggerFactory.getLogger(this.getClass)
  val hiveContext = SparkHelper.hiveContext

  def basicChartEventArea(orgName: String) = {
    val ips = hiveContext.sql(s"select ip from ${orgName}_${ConfigurationHelper.realtimeDataTable}")

    val ipToCityDF = ips.map { x =>
      val ipValue = x.get(0).asInstanceOf[String]
      IpResolutionService.geoDataFromMem(ipValue).city
//      Try() match {
//        case Failure(e) =>
//          logger.error(s"parse ip fail - ${ipValue}", e)
//          None
//        case Success(rst) =>
//          Some(rst)
//      }
    }

    val cityAndCount = ipToCityDF.
//      filter(_.isDefined).
//      map(_.get).
      groupBy("value").count()

    val longLats = LangLatDao.findAll()

    val matchLongLat = cityAndCount.map(cc => {
      val city = cc.get(0).asInstanceOf[String]
      val count = cc.get(1).asInstanceOf[Long]

      val ll = longLats.find(_.city == city)
      (city, ll.map(x => x.long).getOrElse(0.0d), ll.map(x => x.lat).getOrElse(0.0d), count)
    })

    val matchLongLatLocal = matchLongLat.head(10000)

    val resultLoad = ChartSchemaResponse(
      tableName = "eventArea",
      dimensions = "city" :: Nil,
      indexes = "longitude" :: "latitude" :: "count" :: Nil,
      indexTotals = matchLongLatLocal.map(_._4).sum :: Nil,
      load = matchLongLatLocal.map(x => List(x._1, x._2, x._3, x._4)).toList
    )

    resultLoad
  }

//  def basicChartDataSource(orgName: String, orgInfo: String, dataResources: List[DataResourceEntity]) = {
//    val resourceIds = dataResources.map(_.id)
//
//    val resources = resourceIds.filter(k => DataResourceDao.getAllEntity.exists(_.id == k)).map(id => {
//      val e = dataResources.find(_.id == id).get
//      val countRst = e.sourceType match {
//        case "APEX_PRISM" =>
//          val count = SparkHelper.hiveContext.sql(s"select count(*) from ${orgName}_prism_realtime_data_test").map{x =>
//            x.get(0).asInstanceOf[Long]
//          }
//        case "Excel" =>
//          DataResourceEntity
//
//      }
//    })
//
//
//    ???
//  }

}
