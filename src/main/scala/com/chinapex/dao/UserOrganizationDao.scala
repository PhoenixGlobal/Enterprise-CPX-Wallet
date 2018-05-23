package com.chinapex.dao

import com.chinapex.publictools.dao
import net.liftweb.json.DefaultFormats
import net.liftweb.json._
import net.liftweb.json.Extraction
import collection.JavaConversions._

/**
  *
  */
object UserOrganizationDao {

  def getAllEntity = {
    val result = dao.platformDb.queryForList("Select CONTENT from UserOrganizationEntity").map(_.toMap).map(x => {
      x("CONTENT").asInstanceOf[String]
    })
    result.map(x => Extraction.extract[UserOrganizationEntityToSave](parse(x)))
  }
}

case class UserOrganizationEntityToSave(organizationName: String,
                                        organizationID: String,
                                        dataResources: List[String],
                                        isSyncPrismData: Option[Boolean] = None,
                                        labels: List[String],
                                        audiences: List[String],
                                        events: Option[List[String]],
                                        channels: Option[List[String]],
                                        prismToken: String,
                                        supportedDataResources: Option[List[String]],
                                        wisemediaId: Option[String],
                                        biddingxId: Option[String],
                                        zhiziyunId: Option[String],
                                        paiyueId: Option[String],
                                        useFakePreview: Option[Boolean]
                                       )
