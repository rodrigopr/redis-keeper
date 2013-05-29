package br.com.jusbrasil.redis.keeper

import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.Constructor
import akka.actor.{Props, ActorSystem}
import com.typesafe.config.ConfigFactory
import scala.collection.JavaConversions._
import scala.concurrent.duration._
import br.com.jusbrasil.redis.keeper.ClusterMeta.ClusterDefinition
import br.com.jusbrasil.redis.keeper.KeeperMeta.KeeperConfiguration

/** Test class */
object Main extends App {
  private val clusterConfig = getClusterConfig
  private val system = ActorSystem("Keeper")
  updateKeeperId()

  val leaderActor = system.actorOf(Props(new KeeperActor(clusterConfig)), "keeper")

  startLeader()
  scheduleTicks()

  def startLeader() {
    val clusters = clusterConfig.clusters.map(createClusterDef).toList
    val leader = new LeaderProcessor(clusters, leaderActor)
    leader.runAsyncElection()
    leader.ensureInitialized()

    leaderActor ! KeeperConfiguration(leader, clusters)
  }

  def updateKeeperId() {
    val akkaConfig = ConfigFactory.load()

    val hostname = akkaConfig.getString("akka.remote.netty.hostname")
    val port = akkaConfig.getInt("akka.remote.netty.port")
    Keeper.id = "%s:%d".format(hostname, port)
  }

  def scheduleTicks() {
    import system.dispatcher

    val tickInterval: FiniteDuration = Duration(clusterConfig.tick, SECONDS)
    system.scheduler.schedule(0 milliseconds, tickInterval, leaderActor, Tick(tickInterval))

    val failoverTickInterval: FiniteDuration = Duration(clusterConfig.failoverTick, SECONDS)
    system.scheduler.schedule(failoverTickInterval * 2, failoverTickInterval, leaderActor, FailoverTick)
  }

  def createClusterDef(cluster: ClusterConf): ClusterMeta.ClusterDefinition = {
    val nodes = cluster.nodes.map(createNode).toList

    val clusterDef = ClusterDefinition(cluster.name, nodes)
    clusterDef.actor = system.actorOf(Props(new RedisClusterActor(clusterDef)))
    clusterDef.timeToMarkAsDown = cluster.timeToMarkAsDown.seconds
    clusterDef
  }

  def createNode(node: NodeConf): RedisNode = {
    val redisNode = RedisNode(node.host, node.port)
    redisNode.actor = system.actorOf(Props(new RedisNodeWatcherActor(redisNode)))
    redisNode
  }

  def getClusterConfig: Conf = {
    val text =
      """
        |tick: 1
        |failoverTick: 20
        |clusters:
        | - name: webpy
        |   timeToMarkAsDown: 10
        |   nodes:
        |    - {host: 192.168.1.10, port: 6371}
        |    - {host: 192.168.1.10, port: 6372}
        |    - {host: 192.168.1.10, port: 6373}
        |    - {host: 192.168.1.10, port: 6374}
      """.stripMargin
    val yaml = new Yaml(new Constructor(classOf[Conf]))
    yaml.load(text).asInstanceOf[Conf]
  }
}
