package br.com.jusbrasil.redis.keeper

import akka.actor.ActorRef
import br.com.jusbrasil.redis.keeper.KeeperActor.KeeperConfiguration
import com.netflix.curator.framework.recipes.barriers.DistributedBarrier
import com.netflix.curator.framework.recipes.leader.LeaderLatch
import concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import RedisRole._
import org.apache.zookeeper.{WatchedEvent, Watcher}
import org.apache.log4j.Logger
import java.util.concurrent.atomic.AtomicLong
import scala.util.Try

class KeeperProcessor(keeperConfig: KeeperConfig, leaderActor: ActorRef) {
  val id = keeperConfig.keeperId
  private val logger = Logger.getLogger(classOf[KeeperProcessor])
  private val curatorWrapper = new CuratorWrapper(keeperConfig.zkQuorum.mkString(","), keeperConfig.zkPrefix)
  curatorWrapper.init()

  private val initializeBarrier = new DistributedBarrier(curatorWrapper.instance, "/rediskeeper/initialize-barrier")
  private var leaderLatch: LeaderLatch = _

  /**
   * Run election asynchronously,
   *
   * When acquire the leadership it will configure the cluster on ZK, and notify the keeperActor.
   */
  def runAsyncElection() {
    leaderLatch = new LeaderLatch(curatorWrapper.instance, "/rediskeeper/leader", keeperConfig.keeperId)
    leaderLatch.start()

    Future {
      leaderLatch.await()

      try {
        numParticipants.set(leaderLatch.getParticipants.size())

        val watcher = new Watcher {
          override def process(event: WatchedEvent) {
            try {
              numParticipants.set(leaderLatch.getParticipants.size())
            } catch{ case ex: Exception =>
              logger.error("An error occurred updating number of online keepers.", ex)
            }
          }
        }
        curatorWrapper.instance.getChildren.usingWatcher(watcher).inBackground().forPath("/leader")

        configure()
        leaderActor ! KeeperConfiguration(this, keeperConfig.clusters)
      } catch {
        // If failed to configure, should leave the leadership and try again
        case e: Exception =>
          leaderLatch.close()
          runAsyncElection()
      }
    }
  }

  def isLeader = leaderLatch.hasLeadership
  private val numParticipants = new AtomicLong(0)

  /**
   * Wait until the leader is elected, and it configures the cluster structure on ZK
   */
  def ensureInitialized() {
    do {
      Thread.sleep(500)
    } while (!leaderLatch.getLeader.isLeader)

    Thread.sleep(500)

    initializeBarrier.waitOnBarrier()
    //TODO: check for configuration on ZK, the leader`s configuration should match.
  }

  def executeClusterProcess(cluster: ClusterDefinition, failover: Process) {
    assert(isLeader, "keeper %s is not the leader".format(keeperConfig.keeperId))
    try {
      /**
       * TODO: Better failover transaction using ZK,
       * the future leader will want to know that a failover was
       * in progress before he takeover the leadership
       */

      failover.begin()

      try {
        import argonaut.Argonaut._
        val jsonStatus = cluster.getCurrentStatus.asJson.nospaces
        curatorWrapper.createOrSetZkData("/clusters/%s".format(cluster.name), jsonStatus)
      } catch {
        case e: Exception =>
          logger.error("Error updating cluster %s status".format(cluster), e)
      }
    } finally {
      cluster.actor ! FailoverFinished
    }
  }

  /**
   * Get what is the consensus about the node status between the online keepers,
   * @return a tuple (hasConsensusIsDown: Boolean, actualRoleInZK: RedisRole)
   */
  def getConsensusAboutNodeStatus(cluster: ClusterDefinition, node: RedisNode) = {
    val path = RedisNode.statusPath(cluster, node)
    val whoMarkAsDown = curatorWrapper.getChildren(path)

    val hasConsensusIsDown = whoMarkAsDown.size >= numParticipants.get / 2.0
    val actualRole = RedisRole.withName(curatorWrapper.getData(path))

    (hasConsensusIsDown, actualRole)
  }

  def updateNodeStatusOnZK(cluster: ClusterDefinition, node: RedisNode, isOnline: Boolean) {
    val path: String = RedisNode.offlinePath(cluster, node, keeperConfig.keeperId)
    if(isOnline) {
      curatorWrapper.deleteZkData(path)
    } else {
      val timeStr = node.status.lastSeenOnline.getTime.toString
      curatorWrapper.createOrSetZkData(path, timeStr, ephemeral=true)
    }
  }

  def updateNodeRoleOnZK(cluster: ClusterDefinition, node: RedisNode, newRole: RedisRole) {
    val path: String = RedisNode.statusPath(cluster, node)
    curatorWrapper.createOrSetZkData(path, newRole.toString)
  }

  def shutdown() {
    leaderLatch.close()
    curatorWrapper.stop()
  }

  /**
   * Configure the redis cluster on ZK
   * Creates a barrier while doing it, so that others keepers wait until this process finishes.
   */
  private def configure() {
    assert(isLeader, "Configuring without being leader")
    initializeBarrier.setBarrier()

    curatorWrapper.ensureZKPath("/clusters")

    def configNode(cluster: ClusterDefinition)(node: RedisNode) {
      val path = RedisNode.statusPath(cluster, node)
      if (!curatorWrapper.exists(path)) {
        curatorWrapper.createOrSetZkData(path, RedisRole.Undefined.toString)
      } else {
        val role = curatorWrapper.getData(path)
        node.actualRole = RedisRole.withName(role)
      }
    }

    keeperConfig.clusters.foreach { cluster =>
      cluster.nodes.foreach(configNode(cluster))
    }

    initializeBarrier.removeBarrier()
  }

  def curatorSessionInfo = curatorWrapper.sessionInfo
}
