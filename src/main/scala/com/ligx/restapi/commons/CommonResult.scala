package com.ligx.restapi.commons
import spray.json._

/**
  * Created by ligx on 16/8/21.
  */

object CommonResult {

  import DefaultJsonProtocol._
  import JsonUtil.AnyJsonFormat

  def booleanCommonResult(bool: Boolean) = {
    val map = Map[String, Boolean]()
    val new_map = map + ("is_success" -> bool)
    new_map.toJson.prettyPrint
  }

  def mapCommonResult(map: Map[String, Any]) = {
    map.toJson.prettyPrint
  }
}