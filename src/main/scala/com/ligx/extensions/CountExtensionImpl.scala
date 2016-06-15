package com.ligx.extensions

import java.util.concurrent.atomic.AtomicLong

import akka.actor.{ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider}

/**
  * Created by ligx on 16/6/15.
  */
class CountExtensionImpl extends Extension {

  private val count = new AtomicLong()

  def increment() = count.incrementAndGet()

  def getCount(): Long = count.get()
}

object CountExtension extends ExtensionId[CountExtensionImpl] with ExtensionIdProvider {
  override def createExtension(system: ExtendedActorSystem): CountExtensionImpl = new CountExtensionImpl

  override def lookup(): ExtensionId[_ <: Extension] = CountExtension
}
