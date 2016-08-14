package com.ligx.mailbox

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.ActorRef
import akka.dispatch.{Envelope, MessageQueue, UnboundedMailbox}

/**
  * Created by ligx on 16/6/15.
  */
class CustomMailbox extends UnboundedMailbox.MessageQueue{

  private val counter = new AtomicInteger

  override def enqueue(receiver: ActorRef, handle: Envelope): Unit = {
    counter.incrementAndGet()
    super.enqueue(receiver, handle)
  }

  override def dequeue(): Envelope = {
    counter.decrementAndGet()
    super.dequeue()
  }

  override def cleanUp(owner: ActorRef, deadLetters: MessageQueue): Unit = {
    counter.set(0)
    super.cleanUp(owner, deadLetters)
  }

  def getMailboxSize: Int = counter.get()
}
