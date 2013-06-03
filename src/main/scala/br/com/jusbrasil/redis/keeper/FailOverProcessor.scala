package br.com.jusbrasil.redis.keeper

import br.com.jusbrasil.redis.keeper.RedisRole._
import org.apache.log4j.Logger
import scala._
import scala.Some

// TODO: improve interface to have more data about the transaction
trait Process {
  protected val logger: Logger
  protected val cluster: ClusterDefinition
  protected val keeper: KeeperProcessor

  def begin(): Boolean

  /**
   * Elects a new master.
   * Use the actual role on zookeeper, num slaves connected and node uptime in consideration.
   *
   * This will actually select the best node and update his role to Master
   * If the elected node fail to change his role, he will be set on the Undefined state,
   * and another node will be chosen, until one node in good state succeed or no node is available.
   *
   * Return the selected Master or None if failed to do so.
   */
  def electBestMaster(candidates: List[RedisNode]): Option[RedisNode] = {
    val slavesPerMaster = groupSlaves(candidates)
    if(!candidates.isEmpty) {
      val sortedMasters = candidates.sortBy { n =>
        val numberOfSlaves = slavesPerMaster.getOrElse(Some(n), Nil).size
        val uptime = n.status.info.getOrElse("uptime_in_seconds", "0").toInt
        val rolePriorityMap = Map(Master -> 0, Slave -> 1, Undefined -> 3, Down -> 4)
        (rolePriorityMap(n.actualRole), -1 * numberOfSlaves, -1 * uptime)
      }

      val master = sortedMasters.find { node =>
        val success = try {
          updateRedisRole(node, RedisRole.Master)
          setSlaveOf(node, None)
          true
        } catch {
          case e: Exception => false
        }

        success
      }

      if(master.isEmpty) {
        logger.error("All nodes failed to become master on cluster %s, will try again later".format(cluster))
      }

      master
    } else {
      logger.error("There is no node available to be master on cluster %s, will try again later".format(cluster))
      None
    }
  }

  /**
   * Change redis role, using the slaveof command.
   */
  protected def setSlaveOf(node: RedisNode, master: Option[RedisNode]) {
    try {
      node.withConnection { connection =>
        connection.slaveof(master.map(m => (m.host, m.port)).getOrElse(None))
        logger.info("[Failover: %s] Setting %s to be slave of %s".format(cluster, node, master))
      }
    } catch {
      case ex: Exception =>
        logger.error("Error setting %s to be slave of %s".format(node, master), ex)
        updateRedisRole(node, RedisRole.Undefined)
    }
  }

  /**
   * Update redis role on ZK
   */
  protected def updateRedisRole(node: RedisNode, newRole: RedisRole) {
    keeper.updateNodeRoleOnZK(cluster, node, newRole)
    node.actualRole = newRole
    logger.debug("[Failover: %s] updated %s role to: %s".format(cluster, node, newRole))
  }

  /**
   * If a master is available, set it reference on slaves nodes.
   */
  protected def updateSlavesReference(nodes: List[RedisNode]) {
    if (cluster.isMasterOnline) {
      nodes.foreach(setSlaveOf(_, cluster.masterOption))
    }
  }

  /**
   * Group slaves by its master.
   */
  protected def groupSlaves(nodes: List[RedisNode]) =
    nodes.groupBy { node =>
      if (node.status.info.getOrElse("role", "undefined") == "slave") {
        val host = node.status.info("master_host")
        val port = node.status.info("master_port").toInt
        val masterId = RedisNode.nodeId(host, port)
        cluster.nodes.find(n => n.id == masterId)
      } else {
        None
      }
    }
}

class InitialSetupProcessor (
    val keeper: KeeperProcessor,
    val cluster: ClusterDefinition,
    nodesOffline: List[RedisNode],
    nodesOnline: List[RedisNode]) extends Process {
  protected val logger = Logger.getLogger(classOf[InitialSetupProcessor])

  /**
   * Will try to identify the redis cluster topology, and do a leader election if the
   * cluster is not configured yet.
   */
  def begin(): Boolean = {
    logger.warn("[Failover: %s] Initiating redis cluster".format(cluster))
    logger.warn("[Failover: %s] Nodes offline: %s".format(cluster, nodesOffline))
    logger.warn("[Failover: %s] Nodes online: %s".format(cluster, nodesOnline))

    nodesOffline.foreach{ node =>
      updateRedisRole(node, Down)
    }

    val masterOnRedisNodes = nodesOnline.filter(n => n.status.info("role") == "master")
    val realMaster = masterOnRedisNodes.filter(n => n.actualRole == Master)

    if(realMaster.size != 1) {
      electBestMaster(nodesOnline)
    }
    logger.info("Initial Master configured for %s".format(cluster.masterOption))

    updateSlavesReference(cluster.slaveNodes)
    true
  }
}

class FailOverProcessor (
    val keeper: KeeperProcessor,
    val cluster: ClusterDefinition,
    goingOffline: List[RedisNode],
    goingOnline: List[RedisNode]) extends Process {
  protected val logger = Logger.getLogger(classOf[FailOverProcessor])

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
      electBestMaster(cluster.slaveNodes)
    }

    val slavesToUpdate = if(doMasterElection) cluster.slaveNodes else goingOnline
    updateSlavesReference(slavesToUpdate)

    !doMasterElection || cluster.isMasterOnline
  }
}
