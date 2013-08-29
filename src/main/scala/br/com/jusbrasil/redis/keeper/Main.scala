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
import com.typesafe.config.{ConfigException, Config, ConfigFactory}
import scalaz.\/
import argonaut.CursorHistory
import scala.util.{Success, Failure, Try}

class Main(val keeperConfig: KeeperConfig) {
  private var isOnline = false
  private implicit var system: ActorSystem = _
  private var keeper: KeeperProcessor = _

  def getKeeperProcessor = keeper

  def start() {
    val baseConfig= ConfigFactory.load()
    val config = Try { baseConfig.getConfig("keeper") }.getOrElse(baseConfig)

    system = ActorSystem("Keeper", config)
    val keeperActor = system.actorOf(Props(new KeeperActor(keeperConfig)), "keeper")

    createLeaderProcessor(keeperActor)
    createRestApi()

    isOnline = true
  }

  def createRestApi() {
    val service = system.actorOf(Props(new KeeperRestService(keeper)), "rest-service")
    IO(Http) ! Http.Bind(service, "0.0.0.0", keeperConfig.restPort)
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
    node.actor = system.actorOf(Props(new RedisWatcherActor(node)).withDispatcher("watcher-dispatcher"))
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

  private val config: String = Source.fromFile(args(0)).mkString
  private val keeperConfig = parseConfiguration(config)

  def parseConfiguration(conf: String): KeeperConfig = {
    import argonaut.Argonaut._

    conf.decode[KeeperConfig].toEither match {
      case Left(parseOrDecodeError) =>
        val errorMsg = parseOrDecodeError.toEither match {
          case Left(errMsg) => errMsg
          case Right((errMsg, cursor)) => "Error decoding `%s`: %s".format(cursor.head.getOrElse("root"), errMsg)
        }

        throw new ConfigurationException(errorMsg)

      case Right(c) => c
    }
  }

  val process = new Main(keeperConfig)
  process.start()
}
