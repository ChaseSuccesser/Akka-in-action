package com.ligx.restapi.exception

/**
  * Created by Administrator on 2016/8/25.
  */
sealed case class RestExceptionFactor (http_code: Int, error_code: Int, detail_msg: String)
object ParamError extends RestExceptionFactor(404, 4040001, "parameter error")
