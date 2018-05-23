package com.chinapex.dao

import com.chinapex.publictools.dao
import com.chinapex.service.ChartSchemaResponse
import net.liftweb.json.Extraction.decompose
import net.liftweb.json.compactRender

import collection.JavaConversions._
import net.liftweb.json._

import scala.collection.immutable.Map

/**
  *
  */
object DashboardDao {

  def saveToDb(chartSchemaResponse: ChartSchemaResponse, orgName: String): Unit = {
    val loadStr = compactRender(decompose(chartSchemaResponse))

    dao.dataServiceDb.
      queryForList(s"select count(*) as count from `BasicChartScheme` where `id` = '${chartSchemaResponse.tableName}' AND `owner` = '$orgName'").
      map(_.toMap).headOption.flatMap(_.get("count")).map(_.asInstanceOf[Long]) match {
      case Some(0L) =>
        // insert
        dao.dataServiceDb.
          execute(s"insert into `BasicChartScheme` (`id`, `owner`, `load`, `updated_at`) Value ('${chartSchemaResponse.tableName}', '$orgName', '$loadStr', ${System.currentTimeMillis()})")
      case Some(_) =>
        // update
        dao.dataServiceDb.
          execute(s"update `BasicChartScheme` set `load` = '$loadStr', `updated_at` = ${System.currentTimeMillis()} where `id` = '${chartSchemaResponse.tableName}' AND `owner` = '$orgName'")
    }

  }

}
