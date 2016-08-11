package com.ligx.mailbox

import akka.actor.{ActorRef, ActorSystem}
import akka.dispatch.{MailboxType, MessageQueue}
import com.ligx.extensions.MailboxExtension
import com.typesafe.config.Config

/**
  * Created by ligx on 16/6/15.
  */
class CustomMailType(setting: ActorSystem.Settings, config: Config) extends MailboxType{
  override def create(owner: Option[ActorRef], system: Option[ActorSystem]): MessageQueue = {
    (owner, system) match {
      case (Some(a), Some(s)) =>
        val mailbox = new CustomMailbox
        MailboxExtension(s).registerMailbox(a, mailbox)
        mailbox
      case _ => throw new Exception("no mailbox owner or system given")
    }
  }
}
