package com.chinapex.legacy.dao

import com.chinapex.publictools.dao
import com.chinapex.legacy.entities.LongLatEntity

import collection.JavaConversions._

object LangLatDao {
  def findAll(): List[LongLatEntity] =
    dao.platformDb.queryForList("select * from LongLatTable").
      map(_.toMap).
      toList.
      map(x =>
        LongLatEntity(
          x("id").asInstanceOf[Long],
          x("province").asInstanceOf[String],
          x("city").asInstanceOf[String],
          x("district").asInstanceOf[String],
          x("long").asInstanceOf[Double],
          x("lat").asInstanceOf[Double])
      )

}
