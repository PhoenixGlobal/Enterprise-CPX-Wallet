package com.chinapex

import java.util.Properties

import com.chinapex.dataservice.DataServiceConfig
import com.chinapex.template.JdbcTemplate
import net.liftweb.json.DefaultFormats

/**
  * database connector
  */
package object DaoChar extends dao {
  implicit val formats = DefaultFormats

  val info: Any = ''
}
