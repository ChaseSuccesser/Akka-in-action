package com.ligx.schedule

import akka.actor.Actor
import akka.actor.Actor.Receive

/**
  * Created by Administrator on 2016/8/19.
  */
class SchedulerActor extends Actor {

  override def receive: Receive = {
    case msg: String => println(msg)
    case _ => println("unknown message")
  }
}
