package com.hnjme.cloud.pubsub

import java.util.concurrent.Executors

import redis.clients.jedis.JedisPool

/**
 * @author: heguangwu@163.com
 * @description:
 * @date: 2019/08/24
 * @modify:
 */
class RedisPublisher(pubSub: PubSub) extends Publisher {
  val jedisPool: JedisPool = new JedisPool(pubSub.server, pubSub.port)

  override def publish(topic: String, message: String): Unit = {
    jedisPool.getResource.publish(topic, message)
  }
}
