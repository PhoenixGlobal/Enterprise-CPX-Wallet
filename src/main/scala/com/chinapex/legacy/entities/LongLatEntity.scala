package com.chinapex.legacy.entities

case class LongLatEntity( id: Long,
                          province: String,
                          city: String,
                          district: String, //区县
                          long: Double, //经度
                          lat: Double //纬度
                        )
