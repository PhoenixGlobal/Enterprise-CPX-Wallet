package com.chinapex.legacy.services.dbservices

import com.chinapex.legacy.clients.dbclients.SqliteClient
import com.chinapex.legacy.dtos.{ActionResult, EntityTable}
import org.slf4j.LoggerFactory

trait SqliteDbService {
    val logger = LoggerFactory.getLogger("sqliteLogger")
    val tableName: String = ""
    val keyName: String = ""

    private def checkResAndReturn(action: String, actionResult: ActionResult): ActionResult = {
        if(!actionResult.isSuccess) logger.error(s"sqlite action: $action fail, reason: ${actionResult.failReason.get}")
        actionResult
    }

    def updateEntity(entity: EntityTable): ActionResult = {
        checkResAndReturn("update entity", {
            if(SqliteClient().getData(tableName, Some(keyName), Some(entity.key)).isSuccess){
                SqliteClient().updateData(tableName, keyName, entity.key, entity.entity)
            } else ActionResult(isSuccess = false, Some(s"Cannot get record of ${entity.key} in sqlite db."))
        })
    }

    def getEntity(token: String): ActionResult = {
        checkResAndReturn("get entity", SqliteClient().getData(tableName, Some(keyName), Some(token)))
    }

    def deleteEntity(token: String): ActionResult = {
        checkResAndReturn("delete entity", SqliteClient().deleteData(tableName, keyName, token))
    }

    def saveEntity(entity: EntityTable): ActionResult = {
        checkResAndReturn("save entity",{if(SqliteClient().getData(tableName, Some(keyName), Some(entity.key)).isSuccess){
            SqliteClient().updateData(tableName, keyName, entity.key, entity.entity)
        } else SqliteClient().insertData(tableName, keyName, entity.key, entity.entity)})
    }

    def getAllEntity: ActionResult = {
        checkResAndReturn("get all entity", SqliteClient().getData(tableName))
    }

}
