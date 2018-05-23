package com.chinapex.actors

import akka.actor.{Actor, PoisonPill, Props}
import com.chinapex.interfaces._
import org.slf4j.LoggerFactory

/**
  * Created by sli on 17-9-5.
  */
class SupporterHolder extends Actor{
  val logger = LoggerFactory.getLogger(this.getClass)

  override def receive: Receive = {
    case SaveSqlToTable(sql, id, org, userName) => {
      val child = context.child("saveActor").getOrElse(
        context.actorOf(Props[SqlSupportActor], name = "saveActor")
      )
      child ! SaveSqlToTableByChild(sql, id, org, userName, sender)
    }
    case CreateHiveExternalTable(sql, tableName) => {
      val child = context.actorOf(Props[SqlSupportActor])
      child ! CreateHiveExternalTableByChild(sql, tableName, sender)
      child ! PoisonPill
    }
    case DropHiveExternalTable(tableName) => {
      val child = context.actorOf(Props[SqlSupportActor])
      child ! DropHiveExternalTableByChild(tableName, sender)
      child ! PoisonPill
    }
    case PreviewTableWithSql(sql, id) => {
      val child = context.actorOf(Props[SqlSupportActor])
      child ! PreviewTableWithSqlByChild(sql, id, sender)
      child ! PoisonPill
    }

    case BuildTempTableInSpark(org, tableName, hTable, columnInfo) => {
      val child = context.actorOf(Props[SqlSupportActor])
      child ! BuildTempTableInSparkByChild(org, tableName, hTable, columnInfo, sender)
      child ! PoisonPill
    }
    case TestSqlRun(sql) => {
      val child = context.actorOf(Props[SqlSupportActor])
      child ! TestSqlRunByChild(sql, sender)
      child ! PoisonPill
    }
    case RunSql(org, sql) => {
      val child = context.actorOf(Props[SqlSupportActor])
      child ! RunSql(org, sql)
      child ! PoisonPill
    }
    case RunCustomerJourneyCalculationLabel(org, id, evs, st, et) => {
      val child = context.child("customerJourneyCalculator").getOrElse(context.actorOf(Props[CustomerJourneySupporterActor], name = "customerJourneyCalculator"))
      child ! RunCustomerJourneyCalculationLabelByChild(org, id, evs, st, et, sender)
    }
    case RunCustomerJourneyCalculationAudience(org, ids, evs, st, et, aid) => {
      val child = context.child("customerJourneyCalculator").getOrElse(context.actorOf(Props[CustomerJourneySupporterActor], name = "customerJourneyCalculator"))
      child ! RunCustomerJourneyCalculationAudienceByChild(org, ids, evs, st, et, aid, sender)
    }
    case RunFunnelCalculation(org, steps, evs, st, et, filters, id, interval) => {
      val child = context.child("funnelCalculator").getOrElse(context.actorOf(Props[FunnelSupporterActor], name = "funnelCalculator"))
      child ! RunFunnelCalculationByChild(org, steps, evs, st, et, filters, id, interval)
    }
    case FunnelGraphCalculation(org, id, stepCount, st, et, x, y, filters, steps, rid) => {
      val child = context.child("funnelCalculator").getOrElse(context.actorOf(Props[FunnelSupporterActor], name = "funnelCalculator"))
      child ! FunnelGraphCalculationByChild(org, id, stepCount, st, et, x, y, filters, steps, rid, sender)
    }
    case FunnelOverviewCalculation(org, id, st, et, stepCount) => {
      val child = context.child("funnelCalculator").getOrElse(context.actorOf(Props[FunnelSupporterActor], name = "funnelCalculator"))
      child ! FunnelOverviewCalculationByChild(org, id, st, et, stepCount, sender)
    }
    case FunnelTimedDetailCalculation(org, id, stepCount, st, et, steps, rid) => {
      val child = context.child("funnelCalculator").getOrElse((context.actorOf(Props[FunnelSupporterActor], name = "funnelCalculator")))
      child ! FunnelTimedDetailCalculationByChild(org, id, stepCount, st, et, steps, rid, sender)
    }
    case FunnelLabelDataCalculation(org) => {
      val child = context.child("funnelCalculator").getOrElse(context.actorOf(Props[FunnelSupporterActor], name = "funnelCalculator"))
      child ! FunnelLabelDataCalculation(org)
    }
    case FunnelDimensioalCalculation(org, id, st, et, stepCount) => {
      val child = context.actorOf(Props[FunnelSupporterActor])
      child ! FunnelDimensioalCalculation(org, id, st, et, stepCount)
    }

  }

}
