package com.chinapex.dao

import com.chinapex.helpers.ConfigurationHelper
import com.chinapex.tools.orm.LabelIDMapDao

/**
  * Created by Bin on 04/09/2017.
  */
object LabelIDMapDaoObj extends LabelIDMapDao(
    ConfigurationHelper.mysqlAccount,
    ConfigurationHelper.mysqlPassword,
    ConfigurationHelper.mysqlAddress) { }
