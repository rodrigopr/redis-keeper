package br.com.jusbrasil.redis.keeper

import akka.actor._
import concurrent.ExecutionContext.Implicits.global
import java.util.Date
import scala.concurrent.{Await, Future}
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
    val redisInfo = Future {
      node.withConnection { connection =>
        connection.info.map(parseInfo).getOrElse(Map())
      }
    }

    try {
      val infoMap = Await.result(redisInfo, timeout)

      // No only the node need to be accessible, but it must not be loading data
      val isOnline = infoMap.get("loading").map("0" ==).getOrElse(false)
      if(isOnline) {
        node.status.lastSeenOnline = new Date
        node.status.info = infoMap
      }
    } catch {
      case e: Exception =>
        logger.trace("Timeout getting info from node %s: %s".format(node, e.getMessage))
    }
  }
}
