package br.com.jusbrasil.redis.keeper

import akka.actor.{FSM, Actor, ActorRef}
import scala.concurrent.duration._
import scala.concurrent.{Future, Await}
import scala.concurrent.ExecutionContext.Implicits.global
import akka.pattern._
import collection.mutable
import scala.util.Try
import org.apache.log4j.Logger

object ClusterMeta {
  // States
  sealed trait ClusterState
  case object Monitoring extends ClusterState
  case object ProcessingFailover extends ClusterState

  // State data
  case class ClusterDefinition(name: String, nodes: List[RedisNode]) {
    var actor: ActorRef = _
    var timeToMarkAsDown: FiniteDuration = _

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
}
import ClusterMeta._

class RedisClusterActor(clusterDef: ClusterDefinition) extends Actor with FSM[ClusterState, ClusterDefinition] {
  private val logger = Logger.getLogger(classOf[RedisClusterActor])
  startWith(Monitoring, clusterDef)

  when(Monitoring) {
    /** Default node status check. Checks all nodes on the cluster and updates the status on ZK */
    case Event(t @ Tick(timeout), _) =>
      val responses: List[Future[Any]] = clusterDef.nodes.map { node =>
        (node.actor ? t)(timeout) recover { case _ => }
      }

      Try { Await.ready(Future.sequence(responses), timeout * 2) }
      updateNodesStatus()
      stay()

    /** Check if need do a failover on the cluster, and change the actor state if so */
    case Event(CheckFailover(leader), _) =>
      val willDoFailover = processFailover(leader)

      if(willDoFailover)
        goto(ProcessingFailover)
      else
        stay()
  }

  /**
   * Wait into this state until it finishes the failover process
   */
  when(ProcessingFailover) {
    case Event(FailoverFinished, _) => goto(Monitoring)
  }

  whenUnhandled {
    case x => logger.warn("%s not handled at state %s".format(x, stateName)); stay()
  }

  /**
   * Check node status and starts a failover process if required
   * The failover itself will run asynchronous.
   * @return true if a failover is required, false otherwise
   */
  def processFailover(leader: LeaderProcessor): Boolean = {
    val goingOffline = mutable.ArrayBuffer[RedisNode]()
    val goingOnline = mutable.ArrayBuffer[RedisNode]()
    val onlineKeepers: Int = leader.numParticipants

    // Check status of each node
    clusterDef.nodes.foreach { node =>
      val path: String = RedisNode.path(clusterDef, node)

      val whoMarkAsDown = CuratorInstance.getChildren(path)
      val hasConsensusIsDown = whoMarkAsDown.size >= (onlineKeepers / 2.0)

      val actualRole = RedisRole.withName(CuratorInstance.getData(path))
      val isDown: Boolean = actualRole == RedisRole.Down
      val isUnassigned = actualRole == RedisRole.Undefined

      if (hasConsensusIsDown) {
        if (!isDown) {
          goingOffline.append(node)
        }
      } else if (isDown || isUnassigned) {
        goingOnline.append(node)
      }
    }

    if (!goingOffline.isEmpty || !goingOnline.isEmpty) {
      val failover = new FailOverProcessor(clusterDef, goingOffline.toList, goingOnline.toList)
      leader.executeFailover(clusterDef, failover)
      true
    } else false
  }

  /**
   * Update nodes status on ZK
   */
  def updateNodesStatus() {
    def isOnline(node: RedisNode): Boolean = {
      val lastSeen = node.status.lastSeenOnline
      (lastSeen.getTime + clusterDef.timeToMarkAsDown.toMillis) >= System.currentTimeMillis
    }

    val offlineNodes = clusterDef.onlineNodes.filterNot(isOnline).toList
    val backOnlineNodes = clusterDef.offlineNodes.filter(isOnline).toList

    offlineNodes.foreach{ node =>
      logger.warn("Node going offline: %s on cluster %s".format(node, clusterDef))
      val timeStr = node.status.lastSeenOnline.getTime.toString
      CuratorInstance.createOrSetZkData(RedisNode.statusPath(clusterDef, node, Keeper.id), timeStr, ephemeral=true)
      node.status.isOnline = false
    }

    backOnlineNodes.foreach{ node =>
      logger.warn("Node going online: %s on cluster %s".format(node, clusterDef))
      CuratorInstance.deleteZkData(RedisNode.statusPath(clusterDef, node, Keeper.id))
      node.status.isOnline = true
    }
  }

  initialize
}