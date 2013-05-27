package br.com.jusbrasil.redis.keeper

import com.netflix.curator.framework.api.PathAndBytesable
import com.netflix.curator.framework.{CuratorFrameworkFactory, CuratorFramework}
import com.netflix.curator.retry.ExponentialBackoffRetry
import org.apache.zookeeper.CreateMode
import collection.JavaConversions._

object CuratorInstance {
  val curator = createCuratorInstance()
  CuratorInstance.ensureZKPath("/rediskeeper")

  private def createCuratorInstance(): CuratorFramework = {
    val curator = CuratorFrameworkFactory.newClient("localhost:2181", new ExponentialBackoffRetry(50, 15))
    curator.start()
    curator.getZookeeperClient.blockUntilConnectedOrTimedOut()
    curator
  }

  def getData(path: String): String = {
    new String(curator.getData.forPath(path))
  }

  def getChildren(path: String): List[String] = {
    curator.getChildren.forPath(path).toList
  }

  def ensureZKPath(path: String) {
    curator.newNamespaceAwareEnsurePath(path).ensure(curator.getZookeeperClient)
  }

  def createOrSetZkData(path: String, value: String, ephemeral: Boolean = false) {
    val creationMode = if(ephemeral) CreateMode.EPHEMERAL else CreateMode.PERSISTENT

    val setOperation: PathAndBytesable[_] = curator.checkExists().forPath(path) != null match {
      case true => curator.setData()
      case _ => curator.create().creatingParentsIfNeeded().withMode(creationMode)
    }

    setOperation.forPath(path, value.getBytes)
  }

  def deleteZkData(path: String) {
    curator.delete().forPath(path)
  }

  def exists(path: String): Boolean = {
    curator.checkExists().forPath(path) != null
  }
}
