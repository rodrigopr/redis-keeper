package br.com.jusbrasil.redis.keeper

import java.util.Date
import argonaut._, Argonaut._
import akka.actor.ActorRef
import com.redis.RedisClient
import scala.concurrent.duration.FiniteDuration

object RedisRole extends Enumeration {
  type RedisRole = Value
  val Master = Value
  val Slave = Value
  val Backup = Value
  val Undefined = Value
  val Down = Value
}

class RedisNodeStatus {
  var isOnline: Boolean = true
  var lastSeenOnline: Date = new Date()
  var lastChecked: Date = new Date()
  var info: Map[String, String] = Map.empty
}
object RedisNodeStatus {
  implicit val dateEncodeJson: EncodeJson[Date] = EncodeJson (
    (d: Date) => ("time" := d.getTime) ->: jEmptyObject
  )

  implicit val dateDecodeJson: DecodeJson[Date] = DecodeJson (
    d => for { time <- (d --\ "time").as[Long]} yield new Date(time)
  )

  def apply(isOnline: Boolean, lastSeenOnline: Date, info: Map[String, String]) = {
    val status = new RedisNodeStatus
    status.isOnline = isOnline
    status.lastSeenOnline = lastSeenOnline
    status.info = info
    status
  }

  def unapply(self: RedisNodeStatus) = Some((self.isOnline, self.lastSeenOnline, self.info))

  implicit def RedisNodeStatusCodecJson: CodecJson[RedisNodeStatus] =
    casecodec3(RedisNodeStatus.apply, RedisNodeStatus.unapply)("is_online", "number", "info")
}

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
    s"/clusters/${cluster.name}/nodes/${node.id}/status"

  def offlinePath(cluster: ClusterDefinition, node: RedisNode, keeperId: String): String =
    s"/clusters/${cluster.name}/nodes/${node.id}/status/$keeperId"

  def detailPath(cluster: ClusterDefinition, node: RedisNode, keeperId: String): String =
    s"/clusters/${cluster.name}/nodes/${node.id}/detail/$keeperId"

  implicit def RedisNodeCodecJson: CodecJson[RedisNode] =
    casecodec2(RedisNode.apply, RedisNode.unapply)("host", "port")
}

case class ClusterDefinition(name: String, nodes: List[RedisNode]) {
  var actor: ActorRef = _
  var timeToMarkAsDown: FiniteDuration = _

  def getCurrentStatus = ClusterStatus(masterOption, slaveNodes)

  /** Filter by role */
  def masterNodes = nodes.filter(_.actualRole == RedisRole.Master)
  def slaveNodes = nodes.filter(_.actualRole == RedisRole.Slave)
  def undefinedNodes = nodes.filter(_.actualRole == RedisRole.Undefined)

  def masterOption = {
    assert(masterNodes.size <= 1, "There is multiple master nodes")
    masterNodes.headOption
  }

  /** Filter by online status */
  def onlineNodes = nodes.filter(_.status.isOnline)
  def offlineNodes = nodes.filterNot(_.status.isOnline)

  /** Checkers for health of the cluster */
  def isMasterOnline = masterOption.isDefined

  override def toString = name
}
object ClusterDefinition {
  implicit def ClusterDefCodecJson: CodecJson[ClusterDefinition] =
    casecodec2(ClusterDefinition.apply, ClusterDefinition.unapply)("name", "nodes")
}

case class KeeperConfig (
  keeperId: String,
  tick: Int,
  failoverTick: Int,
  timeToMarkAsDown: Int,
  zkQuorum: List[String],
  zkPrefix: String,
  restPort: Int,
  clusters: List[ClusterDefinition]
)
object KeeperConfig {
  implicit def KeeperConfigCodecJson: CodecJson[KeeperConfig] =
    casecodec8(KeeperConfig.apply, KeeperConfig.unapply)("keeper-id", "tick-seconds",
                                                         "failover-tick-seconds", "seconds-mark-down",
                                                         "zk-quorum", "zk-prefix", "rest-port", "clusters")
}

case class ClusterStatus(master: Option[RedisNode], slaves: List[RedisNode])
object ClusterStatus {
  implicit def ClusterStatusCodecJson: CodecJson[ClusterStatus] =
    casecodec2(ClusterStatus.apply, ClusterStatus.unapply)("master", "slaves")
}
