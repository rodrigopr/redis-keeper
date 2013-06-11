package br.com.jusbrasil.redis.keeper

import akka.actor._
import org.apache.log4j.Logger

object KeeperActor {
  object KeeperState extends Enumeration {
    type KeeperState = Value
    val StartingState = Value
    val RunningWorkerState = Value
    val StartingLeaderState = Value
    val RunningLeaderState = Value
  }

  sealed trait KeeperData
  case object UninitializedKeeper extends KeeperData
  case class KeeperConfiguration(leader: KeeperProcessor, clusters: List[ClusterDefinition]) extends KeeperData
}
import KeeperActor._
import KeeperActor.KeeperState._

class KeeperActor(conf: KeeperConfig) extends Actor with FSM[KeeperState, KeeperData] {
  private val logger = Logger.getLogger(classOf[KeeperActor])
  startWith(StartingState, UninitializedKeeper)

  /**
   * No special handling on those two states
   */
  when(StartingState)(Map.empty)
  when(RunningWorkerState)(Map.empty)

  /**
   * Keep in this state until a FailOverTick happen, so that
   * the initial state of the redis cluster can be better defined.
   */
  when(StartingLeaderState) {
    case Event(f @ FailoverTick, keeperConf: KeeperConfiguration) =>
      if (!keeperConf.leader.isLeader) {
        goto(RunningWorkerState)
      } else {
        keeperConf.clusters.foreach { cluster =>
          cluster.actor ! InitClusterSetup(keeperConf.leader)
        }
        goto(RunningLeaderState)
      }
  }

  /**
   * Leader specific behavior.
   * Handle failover ticks, propagating it to each cluster
   */
  when(RunningLeaderState) {
    case Event(f @ FailoverTick, keeperConf: KeeperConfiguration) =>
      if (!keeperConf.leader.isLeader) {
        goto(RunningWorkerState)
      } else {
        keeperConf.clusters.foreach { cluster =>
          cluster.actor ! CheckFailover(keeperConf.leader)
        }
        stay()
      }
  }

  /**
   * Default behavior, used in all states
   */
  whenUnhandled {
    /**
     * KeeperConfiguration event is triggered when it is first configured or the leadership changed
     */
    case Event(conf @ KeeperConfiguration(leader, _), _) =>
      val workingState = if (leader.isLeader) StartingLeaderState else RunningWorkerState
      goto(workingState) using conf

    /**
     * Tick event, propagate to all cluster
     */
    case Event(tick: Tick, keeperConf: KeeperConfiguration) =>
      keeperConf.clusters.foreach { cluster =>
        cluster.actor ! tick
      }
      stay()

    case Event(message, keeperConf: KeeperConfiguration) =>
      logger.info("[Keeper-%s] %s not handled at state %s".format(keeperConf.leader.id, message, stateName))
      stay()
  }

  initialize
}
