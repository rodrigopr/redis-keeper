package br.com.jusbrasil.redis.keeper

import org.scalatest.{Suite, AbstractSuite}
import org.apache.log4j.Logger
import com.netflix.curator.test.TestingServer
import scala.util.Try
import org.apache.zookeeper.ZooKeeper

trait KeeperBaseTestSuite extends AbstractSuite { this: Suite =>
  protected val logger = Logger.getLogger(classOf[KeeperBaseTestSuite])

  val node1 = RedisNode("127.0.0.1", 7341)
  val node2 = RedisNode("127.0.0.1", 7342)
  val node3 = RedisNode("127.0.0.1", 7343)
  val node4 = RedisNode("127.0.0.1", 7344)
  val cluster = ClusterDefinition("cluster1", List(node1, node2, node3))

  protected def defaultConfig(keeperId: String, restPort: Int = 86379) = {
    val clusterCopy = cluster.copy(nodes=List(node1.copy(), node2.copy(), node3.copy()))
    val conf = KeeperConfig(keeperId, 1, 5, 2, List(zkQuorum), "/rediskeeper", restPort, List(clusterCopy))
    conf
  }

  private var zkServer: TestingServer = _
  protected var zkClient: CuratorWrapper = _
  private var zkQuorum: String = _

  abstract override def withFixture(test: NoArgTest) {
    restartZookeeperServer()
    try {
      super.withFixture(test)
    }  finally {
      closeZookeeperServer()
    }
  }

  def restartZookeeperServer() {
    closeZookeeperServer()
    zkServer = new TestingServer()
    zkQuorum = zkServer.getConnectString
    zkClient = new CuratorWrapper(zkQuorum)
    zkClient.init()
  }

  def closeZookeeperServer() {
    Try { zkClient.stop() }
    Try { zkServer.stop() }
  }

  def withRedisInstancesOn(ports: Int*)(fn: Map[Int, RedisProcess] => Unit) {
    val processMap = Set(ports:_*).map{ p => (p, new RedisProcess(p)) }.toMap
    try {
      fn(processMap)
    } finally {
      processMap.values.foreach { p =>
        try
          p.stopProcess()
        catch {
          case e: Exception =>
            logger.error(s"Error stopping process $p", e)
            val errLog = p.errorLog.take(20).map(s"[Err-$p] " +).mkString("\n")
            val outLog = p.outputLog.take(20).map(s"[Out-$p] " +).mkString("\n")
            logger.info(s"Last message from $p process: \n$outLog \n$errLog")
        }
      }
    }
  }

  def withRedisInstancesOff(processes: RedisProcess*)(fn: => Unit) {
    processes.foreach(_.stopProcess())
    try {
      fn
    } finally {
      processes.foreach(_.start())
    }
  }

  def withKeepersOn(configs: KeeperConfig*)(fn: Map[String, Main] => Unit) {
    val processMap = configs.map{ c => c.keeperId -> new Main(c) }.toMap
    try {
        processMap.values.foreach(_.start())
      fn(processMap)
    } finally {
      processMap.values.foreach { p =>
        p.shutdown()
      }
    }
  }

  def withKeeperOff(keepers: Main*)(fn: => Unit) {
    keepers.foreach(_.shutdown())
    try {
      fn
    } finally {
      keepers.foreach(_.start())
    }
  }

  def invalidSession(keeper: Main) {
    logger.info("Invaliding session %s".format(keeper.keeperConfig.keeperId))
    val (sessionData, sessionPassword) = keeper.getKeeperProcessor.curatorSessionInfo
    val zk = new ZooKeeper(zkQuorum, 10000, null, sessionData, sessionPassword)
    zk.close()
  }
}
