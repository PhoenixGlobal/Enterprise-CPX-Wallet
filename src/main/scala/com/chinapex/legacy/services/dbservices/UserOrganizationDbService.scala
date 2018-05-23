package com.chinapex.legacy.services.dbservices

object UserOrganizationDbService extends SqliteDbService {
  override val tableName: String = "UserOrganizationEntity"
  override val keyName: String = "USERORGANIZATIONID"
}
