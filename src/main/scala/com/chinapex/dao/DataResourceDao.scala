package com.chinapex.dao

import com.chinapex.publictools.dao
import net.liftweb.json._
import net.liftweb.json.Extraction
import collection.JavaConversions._

/**
  *
  */
object DataResourceDao {
  def getAllEntity = {
    val result = dao.platformDb.queryForList(s"Select CONTENT from DataResources").map(_.toMap).map(x => {
      x("CONTENT").asInstanceOf[String]
    })
    result.map(x => Extraction.extract[DataResourceEntity](parse(x)))
  }

  def getEntity(resourceId: String) = {
    val result = dao.platformDb.queryForList(s"Select CONTENT from DataResources where ID = '$resourceId'").map(_.toMap).map(x => {
      x("CONTENT").asInstanceOf[String]
    })
    result.headOption.map(x => Extraction.extract[DataResourceEntity](parse(x)))
  }

}

case class DataResourceEntity(sourceName: String,
                              sourceType: String,
                              syncFrequency: String,
                              authInfo: String,
                              id: String,
                              orgName : String)
