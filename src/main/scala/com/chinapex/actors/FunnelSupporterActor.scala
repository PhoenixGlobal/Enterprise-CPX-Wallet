package com.chinapex.actors

import akka.actor.Actor
import com.chinapex.interfaces._
import com.chinapex.service.{FunnelService, ReverseLabelPointerService}

/**
  * Created by sli on 17-9-14.
  */
class FunnelSupporterActor extends Actor{
  override def receive: Receive = {
    case RunFunnelCalculationByChild(org, steps, evs, st, et, filters, id, interval) => {
      FunnelService.calculation(org, steps, evs, st, et, filters, id, interval)
//      FunnelService.funnelDimensionalCalculation(org, id, st, et, steps.length)
    }
    case FunnelGraphCalculationByChild(org, id, stepCount, st, et, x, y, filters, steps, rid, actr) => {
      y match {
        case "conversionRate" => {
          val data = FunnelService.funnelGraphCalculator(org, id, stepCount, st, et, x, y, filters, steps).map(z => (z._1, z._2.asInstanceOf[List[StepResult]]))
          actr ! FunnelGraphCalculationConversionRateResult(rid, data)
        }
        case "conversionTime" => {
          val data = FunnelService.funnelGraphCalculator(org, id, stepCount, st, et, x, y, filters, steps).map(z => (z._1, z._2.asInstanceOf[ConversionTiming]))
          actr ! FunnelGraphCalculationConversionTimeResult(rid, data)
        }
        case "customerLoss" => {
          val data = FunnelService.funnelGraphCalculator(org, id, stepCount, st, et, x, y, filters, steps).map(z => (z._1, z._2.asInstanceOf[List[StepResultWithLoss]]))
          actr ! FunnelGraphCalculationConversionRateWithLossResult(rid, data)
        }
      }
    }
    case FunnelOverviewCalculationByChild(org, id, st, et, stepCount, actr) => {
      val data = FunnelService.calculateOverview(org, id, stepCount, st, et)
      actr ! FunnelOverviewCalculationResult(id, data)
    }
    case FunnelTimedDetailCalculationByChild(org, id, stepCount, st, et, steps, rid, actr) => {
      val data = FunnelService.timedDetailCalculation(org, id, stepCount, st, et, steps)
      actr ! FunnelTimedDetailCalculationResult(rid, data)
    }
    case FunnelLabelDataCalculation(org) => {
      ReverseLabelPointerService.ScheduledUpdate(org)
    }
    case FunnelDimensioalCalculation(org, id, st, et, stepCount) => {
      FunnelService.funnelDimensionalCalculation(org, id, st, et, stepCount)
    }
  }
}
