package com.chinapex.legacy.dao

import com.chinapex.publictools.PublicToolsConfig
import com.chinapex.tools.orm.DataBaseConnectionPool

/**
  * Created by pengmingguo on 5/8/17.
  */
object DataBaseConnectionPool extends DataBaseConnectionPool(
  PublicToolsConfig.publicToolsCf.mysqlCf.platformUsername,
  PublicToolsConfig.publicToolsCf.mysqlCf.platformPassword,
  PublicToolsConfig.publicToolsCf.mysqlCf.platformURL)
