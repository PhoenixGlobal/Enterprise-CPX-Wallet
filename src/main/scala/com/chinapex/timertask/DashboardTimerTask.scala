package com.chinapex.timertask

import com.chinapex.dao.{DashboardDao, UserOrganizationDao}
import com.chinapex.dashboard.timertask.{RepeatTask, Task, TaskKey}
import com.chinapex.helpers.ConfigurationHelper
import com.chinapex.service.impl.HiveTableServiceImpl
import com.chinapex.sparksql.DashboardSpark
import org.slf4j.LoggerFactory

import scala.util.{Failure, Try}
import scala.util.control.NonFatal

/**
  *
  */
object DashboardTimerTask {
  private val logger =  LoggerFactory.getLogger(this.getClass)
  val taskKey = TaskKey("DashboardService", Task.delay(1000 * 5))
  val task = new RepeatTask(taskKey,
    Some(-1, 1000 * 60 * 60 * 3L), // 3 hours
    (_, _) => {
      try {
        val orgData = UserOrganizationDao.getAllEntity
        orgData.foreach { orgInfo =>
          val orgName = orgInfo.organizationName

          logger.info("DashboardService.chartTimertask start task for orgName - " + orgName)
          println("DashboardService.chartTimertask start task for orgName - " + orgName)

          //areaEvent
          Try {
            initHiveTable(orgName)

            val result = debug(DashboardSpark.basicChartEventArea(orgName), "basicChartEventArea")
            DashboardDao.saveToDb(result, orgName)
          } match {
            case Failure(e) =>
              logger.error("basicChartEventArea-fail", e)
              print("basicChartEventArea-fail"); e.printStackTrace()
            case _ => Unit
          }

        }
      } catch {
        case NonFatal(e) =>
          logger.error("DashboardService.chartTimertask ", e)
          print("DashboardService.chartTimertask "); e.printStackTrace()

      }
    }
  )

  private def debug[T](action: => T, methodName: String): T = {
    val time = System.nanoTime()
    val rst = try {
      action
    } catch {
      case NonFatal(e) =>
        logger.error(s"debug-$methodName:", e)
        print(s"debug-$methodName:"); e.printStackTrace()
        throw e
    }
    val endTime = System.nanoTime()
    val costTime = (endTime - time) / 1000000
    logger.info(s"basicCharts-methodName: $methodName cost time - $costTime")
    print(s"basicCharts-methodName: $methodName cost time - $costTime")
    rst
  }

  private def initHiveTable(orgName: String) = {
    //create external hive table is not exist
    val sqlStr =
      s"""create external table IF NOT EXISTS `${orgName}_${ConfigurationHelper.realtimeDataTable}` (`original_key` string, `ip` STRING)""" +
      """stored by 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'""" +
      """with serdeproperties ('hbase.columns.mapping' = ':key, cf:ip')""" +
      s"""tblproperties ('hbase.table.name'='${orgName}:${ConfigurationHelper.realtimeDataTable}')"""

    HiveTableServiceImpl.createTable(sqlStr)

  }
}
