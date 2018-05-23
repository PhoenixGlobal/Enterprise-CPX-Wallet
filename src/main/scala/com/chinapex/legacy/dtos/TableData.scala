package com.chinapex.legacy.dtos

/**
  * Created by ning on 16-10-20.
  */
case class TableData(key: String, data: Map[String, String])
case class ActionResult(isSuccess: Boolean, failReason: Option[String] = None, data: Option[Any] = None)
case class InfoList(FL : List[Info],no : String)
case class Info(content : String ,   `val` : String)
/**
  * sql update entity
  * */
case class EntityTable(key : String, entity: String, time : String)

/**
  * tms entity
  * */
case class TmsEntity(msec : String, cuid: String, hostname: String,
                     remote_addr: String, request: String, status: String,
                     http_host: String, request_uri: String, http_referer: String,
                     http_user_agent: String,http_cookie: String,http_x_forwarded_for: String,
                     request_body: String, query_body: String, query_string: String,
                     echo_client_request_headers: String)

/**
  * msg which needs to be analyze(contains identyfy, alias, event)
  * */
case class ConsumerMsg(tid : String,
                       token : String,
                       utc : String,
                       channel : String,
                       ids : String,
                       context: String,
                       `type` : String,
                       version : String,
                       traits : String,
                       userId : String,
                       idfs : String,
                       idfv : String,
                       event : String,
                       properties : String)
