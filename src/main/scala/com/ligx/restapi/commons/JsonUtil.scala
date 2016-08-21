package com.ligx.restapi.commons

import spray.json.{JsBoolean, JsNumber, JsString, JsValue, JsonFormat}

/**
  * Created by ligx on 16/8/21.
  */
object JsonUtil {

  implicit object AnyJsonFormat extends JsonFormat[Any] {
    def write(x: Any) = x match {
      case n: Int => JsNumber(n)
      case s: String => JsString(s)
      case b: Boolean => JsBoolean(b)
    }

    def read(value: JsValue) = value match {
      case JsNumber(n) => n.intValue()
      case JsString(s) => s
      case JsBoolean(f) => f
    }
  }
}
