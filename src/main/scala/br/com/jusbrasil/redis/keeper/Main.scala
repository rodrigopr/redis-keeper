package br.com.jusbrasil.redis.keeper

import akka.actor.{Props, ActorRef, ActorSystem}
import akka.io.IO
import akka.pattern._
import br.com.jusbrasil.redis.keeper.KeeperActor.KeeperConfiguration
import br.com.jusbrasil.redis.keeper.rest.KeeperRestService
import scala.concurrent.duration._
import scala.io.Source
import spray.can.Http
import scala.concurrent.Await
import akka.util.Timeout

class Main(val keeperConfig: KeeperConfig) {
  private var isOnline = false
  private implicit var system: ActorSystem = _
  private var keeper: KeeperProcessor = _

  def getKeeperProcessor = keeper

  def start() {
    system = ActorSystem("Keeper")
    val keeperActor = system.actorOf(Props(new KeeperActor(keeperConfig)), "keeper")

    createLeaderProcessor(keeperActor)
    createRestApi()

    isOnline = true
  }

  def createRestApi() {
    val service = system.actorOf(Props(new KeeperRestService(keeper)), "rest-service")
    IO(Http) ! Http.Bind(service, "localhost", keeperConfig.restPort)
  }

  def shutdown() {
    if (isOnline) {
      keeperConfig.clusters.foreach { c =>
        system.stop(c.actor)
        c.nodes.foreach {node =>
          system.stop(node.actor)
        }
      }

      implicit val timeout: Timeout = 10.seconds
      Await.result(IO(Http) ? Http.CloseAll, timeout.duration)

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
  if (args.isEmpty) {
    println("Usage: bin/redis-keeper.sh path-to-keeper.conf")
    sys.exit(1)
  }

  private val keeperConfig = {
    val conf = Source.fromFile(args(0)).mkString

    import argonaut.Argonaut._
    conf.decodeEither[KeeperConfig].getOrElse(throw new RuntimeException)
  }

  val process = new Main(keeperConfig)
  process.start()
}
