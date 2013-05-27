package br.com.jusbrasil.redis.keeper

import akka.actor.ActorRef
import br.com.jusbrasil.redis.keeper.ClusterMeta._
import br.com.jusbrasil.redis.keeper.KeeperMeta.KeeperConfiguration
import com.netflix.curator.framework.recipes.barriers.DistributedBarrier
import com.netflix.curator.framework.recipes.leader.LeaderLatch
import concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class LeaderProcessor(clusters: List[ClusterDefinition], leaderActor: ActorRef) {
  private val initializeBarrier = new DistributedBarrier(CuratorInstance.curator, "/rediskeeper/initialize-barrier")

  private val leaderLatch = new LeaderLatch(CuratorInstance.curator, "/rediskeeper/leader", Keeper.id)
  leaderLatch.start()

  /**
   * Run election asynchronously,
   *
   * When acquire the leadership it will configure the cluster on ZK, and notify the keeperActor.
   */
  def runAsyncElection() {
    Future {
      leaderLatch.await()

      try {
        configure()
        leaderActor ! KeeperConfiguration(this, clusters)
      } catch {
        // If failed to configure, should leave the leadership
        case e: Exception =>
      }
    }
  }

  def isLeader = leaderLatch.hasLeadership

  def numParticipants = leaderLatch.getParticipants.size()

  /**
   * Configure the redis cluster on ZK
   * Creates a barrier while doing it, so that others keepers wait until this process finishes.
   */
  private def configure() {
    assert(isLeader, "Configuring without being leader")
    initializeBarrier.setBarrier()

    CuratorInstance.ensureZKPath("/rediskeeper/clusters")

    def configNode(cluster: ClusterMeta.ClusterDefinition)(node: RedisNode) {
      val path = RedisNode.path(cluster, node)
      if (!CuratorInstance.exists(path)) {
        CuratorInstance.createOrSetZkData(path, RedisRole.Undefined.toString)
      } else {
        val role = CuratorInstance.getData(path)
        node.actualRole = RedisRole.withName(role)
      }
    }

    clusters.foreach { cluster =>
      cluster.nodes.foreach(configNode(cluster))
    }

    initializeBarrier.removeBarrier()
  }

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

  def executeFailover(cluster: ClusterDefinition, failover: FailOverProcessor) {
    try {
      /**
       * TODO: Better failover transaction using ZK,
       * the future leader will want to know that a failover was
       * in progress before he takeover the leadership
       */

      failover.begin()
    } finally {
      cluster.actor ! FailoverFinished
    }
  }
}