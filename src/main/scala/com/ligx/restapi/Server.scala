package com.ligx.restapi

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer
import spray.json.DefaultJsonProtocol
import spray.json._

/**
  * Created by Administrator on 2016/8/4.
  */
object Server extends App{


  implicit val system = ActorSystem("webserver")
  implicit val materializer = ActorMaterializer()
  implicit val executionContext = system.dispatcher


  val route =
    (path("order"/"hello") & get) {
      parameters("color", 'count){(color, count) =>
        complete(s"$color $count")
      }
    }

  Http().bindAndHandle(route, interface = "localhost", port = 8888)
}
