package com.chinapex.service

import com.chinapex.interfaces.LabelCustomerPortraitService
import com.chinapex.service.impl.LabelCustomerPortraitServiceImpl

object MapRPCService {
  var mapRPCService = Map(classOf[LabelCustomerPortraitService].getName->new LabelCustomerPortraitServiceImpl)
}
