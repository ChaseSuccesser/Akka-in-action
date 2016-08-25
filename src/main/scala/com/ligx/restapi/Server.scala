package com.ligx.restapi

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.directives.ParameterDirectives
import akka.stream.ActorMaterializer
import spray.json.DefaultJsonProtocol
import spray.json._
import ParameterDirectives.ParamMagnet
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model.MessageEntity
import akka.http.scaladsl.server.ExceptionHandler
import com.ligx.restapi.commons.CommonResult
import com.ligx.restapi.exception.{ParamError, RestException}
import com.sun.xml.internal.ws.util.Pool.Marshaller
import scala.collection.mutable

/**
  * Created by Administrator on 2016/8/4.
  */
object Server extends App{


  implicit val system = ActorSystem("webserver")
  implicit val materializer = ActorMaterializer()
  implicit val executionContext = system.dispatcher

  implicit def exceptionHandler: ExceptionHandler = {
    ExceptionHandler {
      case e: RestException =>
        extractRequest { request =>
          val map = mutable.Map[String, Any]()
          val restExceptionFactor = e.restExceptionFactor
          map ++= List("error_code" -> restExceptionFactor.error_code, "request_method" -> request.method, "request_uri" -> request.uri, "detail_msg" -> restExceptionFactor.detail_msg)
          val immutableMap = Map.empty[String, Any] ++ map
          complete(CommonResult.mapCommonResult(immutableMap))
        }
    }
  }

  val route =
    pathPrefix("order") {
      (path("create_order.json") & post) {
        formFieldMap { formFieldMap =>
          def formParamsString(param: (String, String)): String = s"${param._1} = ${param._2}"
          complete(CommonResult.mapCommonResult(formFieldMap))
        }
      } ~
      (path("get_order.json") & get) {
        parameterMap { parameterMap =>
          def queryParamsString(params: (String, String)): String = s"${params._1} = ${params._2}"
          complete(s"get request parameters are: ${parameterMap.map(queryParamsString).mkString(", ")}")
        }
      } ~
      (path("ping") & get) {
        parameterMap { parameterMap =>
          complete(Marshal("pong").to[MessageEntity])
        }
      } ~
      (path("test_exception") & get) {
        failWith(new RestException(ParamError))
      }
    } ~
    (pathPrefix("pay") & get) {
      parameterMap { paramsMap =>
        def paramString(param: (String, String)): String = {
          s"${param._1} = ${param._2}"
        }
        complete(s"${paramsMap.map(paramString).mkString(",")}")
      }
    }

  Http().bindAndHandle(route, interface = "localhost", port = 8888)
}
