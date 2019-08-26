package com.hnjme.cloud.pubsub

import com.hnjme.cloud.common.Utility.createThreadSafeSet

/**
 * @author: heguangwu@163.com
 * @description:
 * @date: 2019/08/24
 * @modify:
 */
case class PubSub(protocol: String, server: String, port: Int)

trait Serializer {
  def serializer[T](data: T): String
}
trait Deserializer {
  def deserializer[T](messag: String): T
}

trait SubscribeListener {
  val topics = createThreadSafeSet[String]()

  def addTopic(topic: String): Boolean = {
    val exist = topics.add(topic)
    if(exist) {
      onTopicAdded(topic)
    }
    exist
  }

  def onTopicAdded(topic: String): Unit
}

trait Publisher {
  def publish(topic: String, message: String)
}
