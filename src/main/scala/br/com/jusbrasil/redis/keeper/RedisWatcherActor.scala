package br.com.jusbrasil.redis.keeper

import akka.actor._
import java.util.Date
import scala.concurrent.duration._

import org.apache.log4j.Logger

class RedisWatcherActor(node: RedisNode) extends Actor {
  private val logger = Logger.getLogger(classOf[RedisWatcherActor])

  def receive = {
    case Tick(t) => updateRedisInfo(t)
  }

  /**
   * Parse redis info response into a Key-Value map,
   * no processing is done in the values
   */
  def parseInfo(info: String): Map[String, String] = {
    val ignoreLines: (String) => Boolean = {
      l => l.trim.isEmpty || l.startsWith("#")
    }

    info.lines.filterNot(ignoreLines).map { item =>
      item.split(":") match {
        case Array(key, value) => Some( (key, value) )
        case _ => None
      }
    }.flatten.toMap
  }

  /**
   * This method updates the local status(lastSeen and redis info) of the redis node being watched
   */
  def updateRedisInfo(timeout: FiniteDuration) {
    try {
      val redisInfo = node.withConnection { connection => parseInfo(connection.info) }

      // No only the node need to be accessible, but it must not be loading data
      val isOnline = redisInfo.get("loading").exists("0" ==)
      if(isOnline) {
        node.status.lastSeenOnline = new Date
        node.status.info = redisInfo
      }

      logger.trace("Everything ok with %s".format(node))
    } catch {
      case e: Exception =>
        logger.debug("Timeout getting info from node %s: %s".format(node, e.getMessage))
    }
  }
}
