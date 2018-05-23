package com.chinapex.actors

import akka.actor.Actor
import com.chinapex.interfaces.{RunCustomerJourneyCalculationAudience, RunCustomerJourneyCalculationAudienceByChild, RunCustomerJourneyCalculationLabel, RunCustomerJourneyCalculationLabelByChild}
import com.chinapex.service.CustomerJourneyService
import org.slf4j.LoggerFactory

/**
  * Created by sli on 17-9-12.
  */
class CustomerJourneySupporterActor extends Actor{
  val logger = LoggerFactory.getLogger(this.getClass)

  override def receive: Receive = {
    case RunCustomerJourneyCalculationAudienceByChild(org, ids, evs, st, et, aid, actr) => {
      CustomerJourneyService.calculation(org, ids, st, et, evs, aid, actr)
    }
    case RunCustomerJourneyCalculationLabelByChild(org, id, evs, st, et, actr) => {
      CustomerJourneyService.calculation(org, id, st, et, evs, actr)
    }
  }

}
