package com.chinapex.service.impl

import com.chinapex.helpers.SparkHelper
import com.chinapex.service.{ChartSchemaResponse, DashboardService}

/**
  * Dashboard service
  */
object DashboardServiceImpl extends DashboardService {
  private val hiveContext = SparkHelper.hiveContext

  override def basicChartDataSource(orgId: String, orgName: String, dataSourceIDs: List[String]): ChartSchemaResponse = {
//    val dataSourceIds = hiveContext.sql(s"select count(*) from $orgName:prism_realtime_data_test")

    ???
  }

  override def basicChartEventArea(orgId: String) = {
    val idWithIps = hiveContext.sql(s"select count(*) from ip")
    idWithIps

    ???
  }
}
