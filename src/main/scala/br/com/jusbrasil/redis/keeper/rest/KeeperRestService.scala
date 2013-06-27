package br.com.jusbrasil.redis.keeper.rest

import spray.routing.HttpService
import spray.routing.Route
import akka.actor.Actor
import argonaut.Argonaut._
import br.com.jusbrasil.redis.keeper.ClusterStatus._
import br.com.jusbrasil.redis.keeper.{ClusterStatus, KeeperProcessor}
import spray.http.{HttpResponse, StatusCodes}

class KeeperRestService(keeper: KeeperProcessor) extends HttpService with Actor {
  implicit def executionContext = actorRefFactory.dispatcher

  def actorRefFactory = context
  def receive = runRoute(route)

  val route =  {
    get {
      pathPrefix("cluster" / Segment) { clusterName =>
        /** Cluster status: /cluster/$name/status */
        path("status") {
          keeper.getClusterStatus(clusterName).map{ clusterStatus =>
            complete { clusterStatus.asJson.nospaces }
          }.getOrElse {
            complete { HttpResponse(StatusCodes.NotFound) }
          }
        } ~
        /** Cluster master: /cluster/$name/master */
        path("master") {
          keeper.getClusterStatus(clusterName).map { clusterStatus =>
            complete { clusterStatus.master.asJson.nospaces }
          }.getOrElse {
            complete { HttpResponse(StatusCodes.NotFound) }
          }
        } ~
        /** Node is writable(master) (should be used by HAPROXY) */
        path("is-writable") {
          headerValueByName("x-haproxy-server-state") { haproxyState =>
            val nodeId = ExtractNodeIdFromHaproxyStatus(haproxyState)

            withClusterStatusOr404(clusterName) { clusterStatus =>
              if(isMaster(clusterStatus, nodeId))
                complete { s"{'is-writable': 'true'}" }
              else
                complete { HttpResponse(StatusCodes.ServiceUnavailable) }
            }
          }
        } ~
        /** Node is readable (should be used by HAPROXY) */
        path("is-readable") {
          headerValueByName("x-haproxy-server-state") { haproxyState =>
            val nodeId = ExtractNodeIdFromHaproxyStatus(haproxyState)

            withClusterStatusOr404(clusterName) { clusterStatus =>
              if(isMaster(clusterStatus, nodeId) || isSlave(clusterStatus, nodeId))
                complete { s"{'is-readable': 'true'}" }
              else
                complete { HttpResponse(StatusCodes.ServiceUnavailable) }
            }
          }
        } ~
        /** Node is writable(master): /cluster/$name/is-master/$nodeId */
        path("is-writable" / Segment) { nodeId: String =>
          withClusterStatusOr404(clusterName) { clusterStatus =>
            val isWritable = isMaster(clusterStatus, nodeId)
            complete { s"{'is-writable': '$isWritable'}" }
          }
        } ~
        /** Cluster is readable: /cluster/$name/is-master/$nodeId */
        path("is-readable" / Segment) { nodeId: String =>
          withClusterStatusOr404(clusterName) { case clusterStatus =>
            val isReadable = isSlave(clusterStatus, nodeId) || isMaster(clusterStatus, nodeId)
            complete { s"{'is-readable': '$isReadable'}" }
          }
        }
      }
    }
  }

  def isMaster(clusterStatus: ClusterStatus, nodeId: String): Boolean =
    clusterStatus.master.exists(m => m.id == nodeId)

  def isSlave(clusterStatus: ClusterStatus, nodeId: String): Boolean =
    clusterStatus.slaves.exists(m => m.id == nodeId)

  def ExtractNodeIdFromHaproxyStatus(haproxyState: String): String = {
    val info = haproxyState.split(";").collect {
      case i if i.contains("=") =>
        val Array(key, value) = i.trim.split("=", 2)
        (key, value)
    }.toMap

    val Array(_, nodeId) = info("name").split("/", 2)
    nodeId
  }

  def withClusterStatusOr404(clusterName: String)(fn: ClusterStatus => Route): Route = {
    keeper.getClusterStatus(clusterName).map { clusterStatus =>
      fn(clusterStatus)
    } getOrElse {
      complete { HttpResponse(StatusCodes.NotFound) }
    }
  }
}
