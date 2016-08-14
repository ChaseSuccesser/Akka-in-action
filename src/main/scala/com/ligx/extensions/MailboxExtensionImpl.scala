package com.ligx.extensions

import java.util.concurrent.ConcurrentHashMap

import akka.actor.{ActorContext, ActorRef, ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider}
import com.ligx.mailbox.CustomMailbox

/**
  * Created by ligx on 16/6/15.
  */
class MailboxExtensionImpl extends Extension{

  private val mailMap = new ConcurrentHashMap[ActorRef, CustomMailbox]()

  def registerMailbox(actorRef: ActorRef, mailbox: CustomMailbox) = mailMap.put(actorRef, mailbox)

  def ungisterMailbox(actorRef: ActorRef) = mailMap.remove(actorRef)

  def getMailboxSize(implicit context: ActorContext): Int = mailMap.get(context.self).getMailboxSize
}

object MailboxExtension extends ExtensionId[MailboxExtensionImpl] with ExtensionIdProvider{
  override def createExtension(system: ExtendedActorSystem): MailboxExtensionImpl = new MailboxExtensionImpl

  override def lookup(): ExtensionId[_ <: Extension] = this
}
