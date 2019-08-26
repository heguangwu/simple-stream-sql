package com.hnjme.cloud.pubsub

import java.util.concurrent.Executors

import org.slf4j.LoggerFactory
import redis.clients.jedis.{JedisPool, JedisPubSub}

/**
 * @author: heguangwu@163.com
 * @description:
 * @date: 2019/08/24
 * @modify:
 */
class RedisSubscribeListener(pubSub: PubSub, handler: (String, String) => Unit) extends JedisPubSub with SubscribeListener {
  val logger = LoggerFactory.getLogger(classOf[RedisSubscribeListener])
  val jedisPool: JedisPool = new JedisPool(pubSub.server, pubSub.port)
  val executors = Executors.newCachedThreadPool()

  /**
   * redis subscribe channel callback function
   * @param channel
   * @param message
   */
  override def onMessage(channel: String, message: String): Unit = {
    logger.debug(s"subscriber receive channel[$channel] message: $message")
    handler(channel, message)
  }

  /**
   *
   * @param channel
   * @param subscribedChannels
   */
  override def onSubscribe(channel: String, subscribedChannels: Int): Unit = {
    logger.debug(s"$channel on subscribe, subscribe channels $subscribedChannels")
  }

  override def onUnsubscribe(channel: String, subscribedChannels: Int): Unit = {
    logger.warn(s"$channel on unsubscribe, subscribe channels $subscribedChannels")
  }

  /**
   * subscribe redis channel
   * @param topic: redis channel name
   */
  override def onTopicAdded(topic: String): Unit = {
    val callBack = this
    val run = new Runnable {
      override def run(): Unit = {
        while (true) {
          val jedis = jedisPool.getResource
          try {
            jedis.subscribe(callBack, topic)
          } catch {
            case e: Throwable =>
              jedis.close()
              logger.warn(s"redis subscribe exception: $e")
          }
        }
      }
    }
    executors.execute(run)
  }
}
