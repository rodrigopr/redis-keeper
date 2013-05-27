package br.com.jusbrasil.redis.keeper

import org.apache.log4j.Logger
import br.com.jusbrasil.redis.keeper.ClusterMeta._
import br.com.jusbrasil.redis.keeper.RedisRole._
import scala.annotation.tailrec

class FailOverProcessor(cluster: ClusterDefinition, goingOffline: List[RedisNode], goingOnline: List[RedisNode]) {
  private val logger = Logger.getLogger(classOf[FailOverProcessor])

  /**
   * Stats the failover.
   *
   * Returns true if the process succeed as a whole.
   * I'll only fail(return false) is the master is/became unavailable and no online node
   * was able to be elected, or just there was no online node.
   *
   * If fail to update a node role, it'll be marked as Undefined, and will monitored further,
   * another attempt to set him a role will be done on the next failover-tick.
   */
  def begin(): Boolean = {
    logger.warn("Starting failover process for cluster %s".format(cluster))
    logger.warn("[Failover: %s] Nodes going offline: %s".format(cluster, goingOffline))
    logger.warn("[Failover: %s] Nodes going online: %s".format(cluster, goingOnline))

    goingOffline.foreach { node =>
      updateRedisRole(node, RedisRole.Down)
    }

    goingOnline.foreach { node =>
      updateRedisRole(node, RedisRole.Slave)
    }

    val doMasterElection = !cluster.isMasterOnline
    if(doMasterElection) {
      electMaster()
    }

    if(cluster.isMasterOnline) {
      if(doMasterElection) {
        cluster.slaveNodes.foreach(setSlaveOf(_, cluster.masterOption))
      } else {
        goingOnline.foreach(setSlaveOf(_, cluster.masterOption))
      }
    }

    !doMasterElection || cluster.isMasterOnline
  }

  /**
   * This will actually select the best node and update his role to Master
   * If the elected node fail to change his role, he will be set on the Undefined state,
   * and another node will be chosen, until one node in good state succeed or no node is available
   */
  @tailrec
  private def electMaster() {
    //TODO: Improve master election(use more info about slaves to choose from)
    def chooseMaster: RedisNode = cluster.slaveNodes.head
    def existsCandidates: Boolean = !cluster.slaveNodes.isEmpty

    if(existsCandidates) {
      val master = chooseMaster

      updateRedisRole(master, RedisRole.Master)
      setSlaveOf(master, None)

      // Check if it was successful
      if(!cluster.isMasterOnline) {
        electMaster()
      }
    } else {
      logger.error("There is no node available to be master on cluster %s, will try again later".format(cluster))
    }
  }

  /**
   * Change redis role, using the slaveof command.
   */
  private def setSlaveOf(node: RedisNode, master: Option[RedisNode]) {
    try {
      node.withConnection { connection =>
        connection.slaveof(master.map(m => (m.host, m.port)).getOrElse(None))
      }
    } catch {
      case ex: Exception =>
        logger.error("Error updating slaveOf %s to %s, reason: %s".format(node, master, ex.getMessage))
        updateRedisRole(node, RedisRole.Undefined)
    }
  }

  /**
   * Update redis role on ZK
   */
  private def updateRedisRole(node: RedisNode, newRole: RedisRole) {
    node.actualRole = newRole

    val path = s"/rediskeeper/clusters/${cluster.name}/nodes/${node.id}"
    CuratorInstance.createOrSetZkData(path, newRole.toString)

    logger.debug("[Failover: %s] updated %s role to: %s".format(cluster, node, newRole))
  }
}
