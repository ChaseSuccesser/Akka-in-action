package com.ligx.restapi.commons

import spray.json.{JsFalse, JsNumber, JsString, JsTrue, JsValue, JsonFormat}

/**
  * Created by ligx on 16/8/21.
  */
object JsonUtil {

  implicit object AnyJsonFormat extends JsonFormat[Any] {
    def write(x: Any) = x match {
      case n: Int => JsNumber(n)
      case s: String => JsString(s)
      case b: Boolean if b => JsTrue
      case b: Boolean if !b => JsFalse
    }

    def read(value: JsValue) = value match {
      case JsNumber(n) => n
      case JsString(s) => s
      case JsTrue => true
      case JsFalse => false
    }
  }
}
