package br.com.jusbrasil.redis.keeper

import akka.actor.{ FSM, Actor }
import scala.concurrent.duration._
import scala.concurrent.{ Future, Await }
import scala.concurrent.ExecutionContext.Implicits.global
import akka.pattern._
import collection.mutable
import scala.util.Try
import org.apache.log4j.Logger

object ClusterActorState extends Enumeration {
  type ClusterActorState = Value
  val Monitoring = Value
  val ProcessingFailover = Value
}
import ClusterActorState._

class RedisClusterActor(cluster: ClusterDefinition, keeper: KeeperProcessor) extends Actor with FSM[ClusterActorState, Unit] {
  private val logger = Logger.getLogger(classOf[RedisClusterActor])
  startWith(Monitoring, Unit)

  when(Monitoring) {
    /** Default node status check. Checks all nodes on the cluster and updates the status on ZK */
    case Event(t @ Tick(timeout), _) =>
      val responses: List[Future[Any]] = cluster.nodes.map { node =>
        (node.actor ? t)(timeout) recover { case _ => }
      }

      Try { Await.ready(Future.sequence(responses), timeout * 2) }
      updateNodesStatus()
      stay()

    /** Check if need do a failover on the cluster, and change the actor state if so */
    case Event(CheckFailover(leader), _) =>
      val willDoFailover = processFailover(leader)

      if (willDoFailover)
        goto(ProcessingFailover)
      else
        stay()

    /** Setup initial cluster configuration, using data gathered from redis cluster */
    case Event(InitClusterSetup(leader), _) =>
      initRedisCluster(leader)
      goto(ProcessingFailover)
  }

  /**
   * Wait into this state until it finishes the failover process
   */
  when(ProcessingFailover, stateTimeout = 5.minute) {
    //TODO: better handle failover timeouts
    case Event(StateTimeout | FailoverFinished, _) => goto(Monitoring)
  }

  whenUnhandled {
    case Event(message, _) =>
      logger.warn("%s not handled at state %s".format(message, stateName))
      stay()
  }

  /**
   * Start the initial setup process, using the online/offline nodes.
   */
  def initRedisCluster(leader: KeeperProcessor) {
    val nodesOffline = mutable.ArrayBuffer[RedisNode]()
    val nodesOnline = mutable.ArrayBuffer[RedisNode]()

    cluster.nodes.foreach { node =>
      val (hasConsensusIsDown, _) = keeper.getConsensusAboutNodeStatus(cluster, node)
      if (hasConsensusIsDown) {
        nodesOffline.append(node)
      } else {
        nodesOnline.append(node)
      }
    }

    val processor = new InitialSetupProcessor(keeper, cluster, nodesOffline.toList, nodesOnline.toList)
    leader.executeClusterProcess(cluster, processor)
  }

  /**
   * Check node status and starts a failover process if required
   * The failover itself will run asynchronous.
   * @return true if a failover is required, false otherwise
   */
  def processFailover(leader: KeeperProcessor): Boolean = {
    val goingOffline = mutable.ArrayBuffer[RedisNode]()
    val goingOnline = mutable.ArrayBuffer[RedisNode]()

    // Check status of each node
    cluster.nodes.foreach { node =>
      val (hasConsensusIsDown, actualRoleZk) = keeper.getConsensusAboutNodeStatus(cluster, node)
      val currentStatusIsDown = actualRoleZk == RedisRole.Down
      val currentStatusIsUndefined = actualRoleZk == RedisRole.Undefined

      if (hasConsensusIsDown) {
        if (!currentStatusIsDown) {
          goingOffline.append(node)
        }
      } else if (currentStatusIsDown || currentStatusIsUndefined) {
        goingOnline.append(node)
      }
    }

    val doFailover = !goingOffline.isEmpty || !goingOnline.isEmpty
    if (doFailover) {
      val failover = new FailOverProcessor(keeper, cluster, goingOffline.toList, goingOnline.toList)
      leader.executeClusterProcess(cluster, failover)
    }

    doFailover
  }

  /**
   * Update nodes status on ZK
   */
  def updateNodesStatus() {
    def isOnline(node: RedisNode): Boolean = {
      val lastSeen = node.status.lastSeenOnline
      lastSeen.getTime + cluster.timeToMarkAsDown.toMillis >= System.currentTimeMillis
    }

    val offlineNodes = cluster.nodes.filterNot(isOnline).toList
    val backOnlineNodes = cluster.offlineNodes.filter(isOnline).toList

    offlineNodes.foreach { node =>
      if (node.status.isOnline) {
        logger.warn("Node going offline: %s on cluster %s for keeper %s".format(node, cluster, keeper.id))
      }
      keeper.updateNodeStatusOnZK(cluster, node, isOnline = false)
      node.status.isOnline = false
    }

    backOnlineNodes.foreach{ node =>
      logger.warn("Node going online: %s on cluster %s for keeper %s".format(node, cluster, keeper.id))
      keeper.updateNodeStatusOnZK(cluster, node, isOnline = true)
      node.status.isOnline = true
    }
  }

  initialize
}
