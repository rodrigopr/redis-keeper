package br.com.jusbrasil.redis.keeper

import org.scalatest._
import com.netflix.curator.test.TestingServer
import scala.sys.process._
import scala.collection.mutable
import org.apache.log4j.Logger
import scala.util.Try
import java.nio.file.Paths
import org.apache.zookeeper.ZooKeeper

class RedisProcess(val port: Int) {
  protected val logger = Logger.getLogger(classOf[RedisProcess])
  private val redisPath = "/usr/local/bin/redis-server"
  val outputLog = new mutable.ListBuffer[String]
  val errorLog = new mutable.ListBuffer[String]

  private val currentRelativePath = Paths.get("")
  private val absolutePath = currentRelativePath.toAbsolutePath.toString
  private val configFile: String = s"$absolutePath/src/test/conf/redis_$port.conf"

  private var running = false
  private var process: scala.sys.process.Process = _
  start()

  def start() {
    if(!running) {
      outputLog.clear()
      errorLog.clear()
      process =
        Seq("sh", "-c", s"$redisPath $configFile").run(new ProcessLogger {
          def buffer[T](f: => T): T = f

          def out(s: => String) {
            outputLog.+=:(s)
          }

          def err(s: => String) {
            errorLog.+=:(s)
            logger.error("[Redis %s] %s".format(port, s))
          }
        })

      running = true
    }
  }


  def stopProcess() {
    Try {
      RedisNode("127.0.0.1", port).withConnection { conn =>
        conn.shutdown
      }
      process.exitValue()
    } recover {
      case _ => process.destroy()
    }
    running = false
  }

  override def toString = s"Redis-$port"
}

trait KeeperBaseTestSuite extends AbstractSuite { this: Suite =>
  protected val logger = Logger.getLogger(classOf[KeeperBaseTestSuite])
  var zkServer: TestingServer = _
  var zkClient: CuratorWrapper = _
  var zkQuorum: String = _

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

class ClusterTestSuite extends FlatSpec with KeeperBaseTestSuite {
  val node1 = RedisNode("127.0.0.1", 7341)
  val node2 = RedisNode("127.0.0.1", 7342)
  val node3 = RedisNode("127.0.0.1", 7343)
  val node4 = RedisNode("127.0.0.1", 7344)
  val cluster = ClusterDefinition("cluster1", List(node1, node2, node3))

  "RedisKeeper" should "detect offline nodes" in {

    withRedisInstancesOn(7341) { _ =>
      val conf = defaultConfig("keeper1")

      withKeepersOn(conf) { _ =>
        Thread.sleep(15000)

        val detectedAsDown1 = zkClient.getChildren(RedisNode.statusPath(cluster, node1))
        assert(detectedAsDown1.isEmpty)
        val detectedAsDown2 = zkClient.getChildren(RedisNode.statusPath(cluster, node2))
        assert(detectedAsDown2.size == 1)

        assertNodeRole(cluster, node1, RedisRole.Master.toString)
        assertNodeRole(cluster, node2, RedisRole.Down.toString)

        withRedisInstancesOn(7342) { _ =>
          Thread.sleep(10000)

          val detectedAsDown1 = zkClient.getChildren(RedisNode.statusPath(cluster, node1))
          assert(detectedAsDown1.isEmpty)
          val detectedAsDown2 = zkClient.getChildren(RedisNode.statusPath(cluster, node2))
          assert(detectedAsDown2.isEmpty)

          assertNodeRole(cluster, node1, RedisRole.Master.toString)
          assertNodeRole(cluster, node2, RedisRole.Slave.toString)
        }
      }
    }
  }

  "RedisKeeper" should "should work with multiple keepers" in {
    withRedisInstancesOn(7341) { _ =>

      val conf = defaultConfig("keeper1")
      val conf2 = defaultConfig("keeper2")
      val conf3 = defaultConfig("keeper3")

      withKeepersOn(conf, conf2, conf3) { _ =>
        Thread.sleep(15000)

        val detectedAsDown1 = zkClient.getChildren(RedisNode.statusPath(cluster, node1))
        assert(detectedAsDown1.isEmpty)
        val detectedAsDown2 = zkClient.getChildren(RedisNode.statusPath(cluster, node2))
        assert(detectedAsDown2.size == 3)

        assertNodeRole(cluster, node1, RedisRole.Master.toString)
        assertNodeRole(cluster, node2, RedisRole.Down.toString)

        withRedisInstancesOn(7342) { _ =>
          Thread.sleep(10000)

          val detectedAsDown1 = zkClient.getChildren(RedisNode.statusPath(cluster, node1))
          assert(detectedAsDown1.isEmpty)
          val detectedAsDown2 = zkClient.getChildren(RedisNode.statusPath(cluster, node2))
          assert(detectedAsDown2.isEmpty)

          assertNodeRole(cluster, node1, RedisRole.Master.toString)
          assertNodeRole(cluster, node2, RedisRole.Slave.toString)
        }
      }
    }
  }

  "RedisKeeper" should "should elect a new master in case of failure" in {
    withRedisInstancesOn(7341, 7343) { redisInstances =>

      val conf = defaultConfig("keeper1")
      val conf2 = defaultConfig("keeper2")
      val conf3 = defaultConfig("keeper3")

      withKeepersOn(conf, conf2, conf3) { _ =>
        Thread.sleep(15000)

        assertNodeRole(cluster, node1, RedisRole.Master.toString)
        assertNodeRole(cluster, node2, RedisRole.Down.toString)
        assertNodeRole(cluster, node3, RedisRole.Slave.toString)

        withRedisInstancesOn(7342) { _ =>
          Thread.sleep(10000)

          assertNodeRole(cluster, node1, RedisRole.Master.toString)
          assertNodeRole(cluster, node2, RedisRole.Slave.toString)
          assertNodeRole(cluster, node3, RedisRole.Slave.toString)

          withRedisInstancesOff(redisInstances(7341)) {
            Thread.sleep(10000)

            assertNodeRole(cluster, node1, RedisRole.Down.toString)
            assertNodeRole(cluster, node2, RedisRole.Slave.toString)
            assertNodeRole(cluster, node3, RedisRole.Master.toString)
          }

          Thread.sleep(10000)

          assertNodeRole(cluster, node1, RedisRole.Slave.toString)
          assertNodeRole(cluster, node2, RedisRole.Slave.toString)
          assertNodeRole(cluster, node3, RedisRole.Master.toString)
        }
      }
    }
  }

  "RedisKeeper" should "handle session expiration" in {
    withRedisInstancesOn(7341, 7343) { redisInstances =>

      val conf = defaultConfig("keeper1")
      val conf2 = defaultConfig("keeper2")

      withKeepersOn(conf) { keepers =>
      // Running 2 after to guarantee that keeper1 will get the leadership
        withKeepersOn(conf2) { _ =>
          Thread.sleep(20000)
          assertNodeRole(cluster, node1, RedisRole.Master.toString)
          assertNodeRole(cluster, node2, RedisRole.Down.toString)
          assertNodeRole(cluster, node3, RedisRole.Slave.toString)

          invalidSession(keepers("keeper1"))

          withRedisInstancesOff(redisInstances(7341)) {
            Thread.sleep(20000)

            assertNodeRole(cluster, node1, RedisRole.Down.toString)
            assertNodeRole(cluster, node2, RedisRole.Down.toString)
            assertNodeRole(cluster, node3, RedisRole.Master.toString)
          }
        }
      }
    }
  }

  "RedisKeeper" should "adapt to the number of keepers online" in {
    withRedisInstancesOn(7341, 7343) { redisInstances =>

      val conf = defaultConfig("keeper1")
      val conf2 = defaultConfig("keeper2")
      val conf3 = defaultConfig("keeper3")
      val conf4 = defaultConfig("keeper4")

      withKeepersOn(conf, conf2, conf3, conf4) { keepers =>
        Thread.sleep(20000)
        assertNodeRole(cluster, node1, RedisRole.Master.toString)
        assertNodeRole(cluster, node2, RedisRole.Down.toString)
        assertNodeRole(cluster, node3, RedisRole.Slave.toString)

        withKeeperOff(keepers("keeper2"), keepers("keeper3"), keepers("keeper4")) {
          withRedisInstancesOff(redisInstances(7341)) {
            Thread.sleep(10000)

            assertNodeRole(cluster, node1, RedisRole.Down.toString)
            assertNodeRole(cluster, node2, RedisRole.Down.toString)
            assertNodeRole(cluster, node3, RedisRole.Master.toString)
          }

          Thread.sleep(10000)

          assertNodeRole(cluster, node1, RedisRole.Slave.toString)
          assertNodeRole(cluster, node2, RedisRole.Down.toString)
          assertNodeRole(cluster, node3, RedisRole.Master.toString)
        }

        Thread.sleep(10000)
        assertNodeRole(cluster, node1, RedisRole.Slave.toString)
        assertNodeRole(cluster, node2, RedisRole.Down.toString)
        assertNodeRole(cluster, node3, RedisRole.Master.toString)

        withRedisInstancesOff(redisInstances(7341)) {
          Thread.sleep(10000)

          assertNodeRole(cluster, node1, RedisRole.Down.toString)
          assertNodeRole(cluster, node2, RedisRole.Down.toString)
          assertNodeRole(cluster, node3, RedisRole.Master.toString)
        }
      }
    }
  }


  def assertNodeRole(cluster: ClusterDefinition, node: RedisNode, expectedRole: String) {
    val role = zkClient.getData(RedisNode.statusPath(cluster, node))
    assert(role === expectedRole)
  }

  private def defaultConfig(keeperId: String) = {
    val clusterCopy = cluster.copy(nodes=List(node1.copy(), node2.copy(), node3.copy()))
    val conf = KeeperConfig(keeperId, 1, 5, 2, List(zkQuorum), List(clusterCopy))
    conf
  }
}
