package br.com.jusbrasil.redis.keeper

import akka.actor._
import org.apache.log4j.Logger

object KeeperMeta {
  sealed trait KeeperState
  case object StartingState extends KeeperState
  case object RunningWorkerState extends KeeperState
  case object RunningLeaderState extends KeeperState

  sealed trait KeeperData
  case object UninitializedKeeper extends KeeperData
  case class KeeperConfiguration(leader: LeaderProcessor, clusters: List[ClusterMeta.ClusterDefinition]) extends KeeperData
}
import KeeperMeta._

class KeeperActor(conf: Conf) extends Actor with FSM[KeeperState, KeeperData] {
  private val logger = Logger.getLogger(classOf[KeeperActor])
  startWith(StartingState, UninitializedKeeper)

  /**
   * No special handling on those two states
   */
  when(StartingState)(Map.empty)
  when(RunningWorkerState)(Map.empty)

  /**
   * Leader specific behavior.
   * Handle failover ticks, propagating it to each cluster
   */
  when(RunningLeaderState) {
    case Event(f @ FailoverTick, keeperConf: KeeperConfiguration) =>
      if(!keeperConf.leader.isLeader) {
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
      val workingState = if(leader.isLeader) RunningLeaderState else RunningWorkerState
      goto(workingState) using conf

    /**
     * Tick event, propagate to all cluster
     */
    case Event(tick: Tick, keeperConf: KeeperConfiguration) =>
      keeperConf.clusters.foreach { cluster =>
        cluster.actor ! tick
      }
      stay()

    case x => logger.info("%s not handled at state %s".format(x, stateName)); stay()
  }

  initialize
}

object Keeper {
  var id: String = _
}