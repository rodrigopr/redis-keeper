package br.com.jusbrasil.redis.keeper

import com.netflix.curator.framework.api.PathAndBytesable
import com.netflix.curator.framework.CuratorFrameworkFactory
import com.netflix.curator.retry.ExponentialBackoffRetry
import org.apache.zookeeper.CreateMode
import collection.JavaConversions._
import org.apache.zookeeper.KeeperException.NoNodeException

class CuratorWrapper(zkQuorum: String, prefix: String = "/rediskeeper") {
  val instance = CuratorFrameworkFactory.newClient(zkQuorum, new ExponentialBackoffRetry(50, 10))

  def init() {
    instance.start()
    instance.getZookeeperClient.blockUntilConnectedOrTimedOut()

    ensureZKPath("")
  }

  def buildPath(path: String) = {
    val suffix = if (!path.startsWith("/") && !path.isEmpty) "/" + path else path
    prefix + suffix
  }

  def getData(path: String): String = {
    new String(instance.getData.forPath(buildPath(path)))
  }

  def getChildren(path: String): List[String] = {
    instance.getChildren.forPath(buildPath(path)).toList
  }

  def ensureZKPath(path: String) {
    instance.newNamespaceAwareEnsurePath(buildPath(path)).ensure(instance.getZookeeperClient)
  }

  def createOrSetZkData(path: String, value: String, ephemeral: Boolean = false) {
    val creationMode = if (ephemeral) CreateMode.EPHEMERAL else CreateMode.PERSISTENT

    val setOperation: PathAndBytesable[_] = instance.checkExists().forPath(buildPath(path)) != null match {
      case true => instance.setData()
      case _ => instance.create().creatingParentsIfNeeded().withMode(creationMode)
    }

    setOperation.forPath(buildPath(path), value.getBytes)
  }

  def deleteZkData(path: String) {
    try {
      instance.delete().forPath(buildPath(path))
    } catch { case e: NoNodeException => }
  }

  def exists(path: String): Boolean = {
    instance.checkExists().forPath(buildPath(path)) != null
  }

  /**
   * Returns a tuple (sessionId: Long, sessionPassword: Array[Byte])
   */
  def sessionInfo = {
    val connection = instance.getZookeeperClient.getZooKeeper
    (connection.getSessionId, connection.getSessionPasswd)
  }

  def stop() {
    instance.close()
  }
}
