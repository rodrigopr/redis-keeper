package br.com.jusbrasil.redis.keeper

import java.util.Date
import argonaut._, Argonaut._


object KeeperMode extends Enumeration{
  type KeeperMode = Value
  val Starting = Value
  val Started = Value
}

object ClusterStatus extends Enumeration {
  type ClusterStatus = Value
  val FailOverInProcess = Value
  val Online = Value
  val Offline = Value
}

object RedisRole extends Enumeration {
  type RedisRole = Value
  val Master = Value
  val Slave = Value
  val Backup = Value
  val Undefined = Value
  val Down = Value
}

case class RedisNodeStatus(
  var isOnline: Boolean = true,
  var lastSeenOnline: Date = new Date(),
  var info: Map[String, String] = Map()
)

object RedisNodeStatus {
  implicit val dateEncodeJson: EncodeJson[Date] = EncodeJson (
    (d: Date) => ("time" := d.getTime) ->: jEmptyObject
  )

  implicit val dateDecodeJson: DecodeJson[Date] = DecodeJson (
    d => for { time <- (d --\ "time").as[Long]} yield new Date(time)
  )

  implicit def RedisNodeStatusCodecJson: CodecJson[RedisNodeStatus] =
    casecodec3(RedisNodeStatus.apply, RedisNodeStatus.unapply)("is_online", "number", "info")
}

import scala.beans.BeanProperty

class NodeConf {
  @BeanProperty
  var host: String = _

  @BeanProperty
  var port: Int = _

  override def toString = "{host: %s, port: %s}".format(host, port)
}

class ClusterConf {
  @BeanProperty
  var name: String = _

  @BeanProperty
  var timeToMarkAsDown: Int = _

  @BeanProperty
  var nodes: java.util.List[NodeConf] = _

  override def toString = "{name: %s, nodes: %s}".format(name, nodes)
}

class Conf {
  @BeanProperty
  var clusters: java.util.List[ClusterConf] = _

  @BeanProperty
  var tick: Int = 1

  @BeanProperty
  var failoverTick: Int = 5

  override def toString = clusters.toString
}