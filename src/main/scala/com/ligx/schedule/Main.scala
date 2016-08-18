package com.ligx.schedule

import akka.actor.{ActorSystem, Props}

import scala.concurrent.duration._

/**
  * Created by Administrator on 2016/8/19.
  */
object Main extends App {

  val system = ActorSystem("ScheduleSystem")
  val scheduleActor = system.actorOf(Props[SchedulerActor], "scheduleActor")
  implicit val ec = system.dispatcher

  system.scheduler.schedule(1 second, 1 second, scheduleActor, "hello")
}
