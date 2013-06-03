package br.com.jusbrasil.redis.keeper

import akka.actor.{ActorRef, Props, ActorSystem}
import scala.concurrent.duration._
import br.com.jusbrasil.redis.keeper.KeeperActor.KeeperConfiguration

class Main(val keeperConfig: KeeperConfig) {
  private var isOnline = false
  private var system: ActorSystem = _
  private var keeper: KeeperProcessor = _

  def getKeeperProcessor = keeper

  def start() {
    system = ActorSystem("Keeper")
    val keeperActor = system.actorOf(Props(new KeeperActor(keeperConfig)), "keeper")

    createLeaderProcessor(keeperActor)

    isOnline = true
  }

  def shutdown() {
    if(isOnline) {
      keeperConfig.clusters.foreach { c =>
        system.stop(c.actor)
        c.nodes.foreach {node =>
          system.stop(node.actor)
        }
      }
      system.shutdown()
      system.awaitTermination()
      keeper.shutdown()
      isOnline = false
    }
  }

  private def createLeaderProcessor(keeperActor: ActorRef): KeeperProcessor = {
    keeper = new KeeperProcessor(keeperConfig, keeperActor)
    keeperConfig.clusters.foreach(initCluster(keeper, _))

    keeper.runAsyncElection()
    keeper.ensureInitialized()

    keeperActor ! KeeperConfiguration(keeper, keeperConfig.clusters)
    scheduleTicks(system, keeperActor)
    keeper
  }

  private def initCluster(keeper: KeeperProcessor, cluster: ClusterDefinition) {
    cluster.nodes.foreach(initNode)
    cluster.actor = system.actorOf(Props(new RedisClusterActor(cluster, keeper)))
    cluster.timeToMarkAsDown = keeperConfig.timeToMarkAsDown.seconds
  }

  private def initNode(node: RedisNode) {
    node.actor = system.actorOf(Props(new RedisWatcherActor(node)))
  }

  private def scheduleTicks(system: ActorSystem, keeperActor: ActorRef) {
    import system.dispatcher

    val tickInterval: FiniteDuration = Duration(keeperConfig.tick, SECONDS)
    system.scheduler.schedule(0 milliseconds, tickInterval, keeperActor, Tick(tickInterval))

    val failoverTickInterval: FiniteDuration = Duration(keeperConfig.failoverTick, SECONDS)
    system.scheduler.schedule(failoverTickInterval * 2, failoverTickInterval, keeperActor, FailoverTick)
  }
}


/** Test class */
object Main extends App {
  private val keeperConfig = getClusterConfig

  val process = new Main(keeperConfig)
  process.start()

  def getClusterConfig: KeeperConfig = {
    val text =
      """
        |{
        | "keeper-id": "keeper-1",
        | "tick": 1,
        | "failover-tick": 20,
        | "clusters": [
        |   {
        |     "name": "webpy",
        |     "nodes": [
        |       {"host": "192.168.1.10", "port": 6371},
        |       {"host": "192.168.1.10", "port": 6372},
        |       {"host": "192.168.1.10", "port": 6373},
        |       {"host": "192.168.1.10", "port": 6374}
        |     ]
        |   }
        | ]
        |}
      """.stripMargin

    import argonaut.Argonaut._
    text.decodeEither[KeeperConfig].getOrElse(throw new RuntimeException)
  }
}
