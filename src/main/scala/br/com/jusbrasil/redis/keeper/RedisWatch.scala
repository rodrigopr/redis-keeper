package br.com.jusbrasil.redis.keeper

import akka.actor._
import concurrent.ExecutionContext.Implicits.global
import com.redis.RedisClient
import java.util.Date
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

import br.com.jusbrasil.redis.keeper.ClusterMeta._
import org.apache.log4j.Logger

case class RedisNode(host: String, port: Int) {
  var actor: ActorRef = _
  val id = "%s:%d".format(host, port)
  val status = new RedisNodeStatus
  var actualRole = RedisRole.Undefined

  /**
   * Creates a connection and call the `fn` method using it.
   * No handling is done to ensure that the connection is healthy.
   * It gets destroyed on the end of the execution
   */
  def withConnection[T](fn: (RedisClient) => T): T = {
    val redisConnection = new RedisClient(host, port)
    try {
      fn(redisConnection)
    } finally {
      redisConnection.disconnect
    }
  }

  override def toString = id
}

object RedisNode {
  def nodeId(host: String, port: Int) = "%s:%d".format(host,port)

  def statusPath(cluster: ClusterDefinition, node: RedisNode): String =
    s"/rediskeeper/clusters/${cluster.name}/nodes/${node.id}/status"

  def detailPath(cluster: ClusterDefinition, node: RedisNode, keeperId: String): String =
    s"/rediskeeper/clusters/${cluster.name}/nodes/${node.id}/detail/$keeperId"

  def offlinePath(cluster: ClusterDefinition, node: RedisNode, keeperId: String): String =
    s"/rediskeeper/clusters/${cluster.name}/nodes/${node.id}/status/$keeperId"


}

class RedisNodeWatcherActor(node: RedisNode) extends Actor {
  private val logger = Logger.getLogger(classOf[RedisNodeWatcherActor])

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
        logger.trace("Timeout getting info from: %s".format(node))
    }
  }
}
