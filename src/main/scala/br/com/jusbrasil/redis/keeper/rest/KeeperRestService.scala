package br.com.jusbrasil.redis.keeper.rest

import spray.routing.HttpService
import akka.actor.Actor
import argonaut.Argonaut._
import br.com.jusbrasil.redis.keeper.ClusterStatus._
import br.com.jusbrasil.redis.keeper.KeeperProcessor
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
        /** Node is master (should be used by HAPROXY) */
        path("is-master") {
          headerValueByName("x-haproxy-server-state") { haproxyState =>
            val nodeId = ExtractNodeIdFromHaproxyStatus(haproxyState)

            keeper.getClusterStatus(clusterName).map { clusterStatus =>
              clusterStatus.master.find(m => m.id == nodeId).map { _ =>
                complete { s"{'is-master': 'true'}" }
              } getOrElse {
                complete { HttpResponse(StatusCodes.ServiceUnavailable) }
              }
            } getOrElse {
              complete { HttpResponse(StatusCodes.NotFound) }
            }
          }
        } ~
        /** Node is slave (should be used by HAPROXY) */
        path("is-slave") {
          headerValueByName("x-haproxy-server-state") { haproxyState =>
            val nodeId = ExtractNodeIdFromHaproxyStatus(haproxyState)
            keeper.getClusterStatus(clusterName).map { clusterStatus =>
              clusterStatus.slaves.find(m => m.id == nodeId).map { _ =>
                complete { s"{'is-slave': 'true'}" }
              } getOrElse {
                complete { HttpResponse(StatusCodes.ServiceUnavailable) }
              }
            } getOrElse {
              complete { HttpResponse(StatusCodes.NotFound) }
            }
          }
        } ~
        /** Node is master: /cluster/$name/is-master/$nodeId */
        path("is-master" / Segment) { nodeId: String =>
          keeper.getClusterStatus(clusterName).map { clusterStatus =>
            val isMaster = clusterStatus.master.exists(m => m.id == nodeId)
            complete { s"{'is-master': '$isMaster'}" }
          } getOrElse {
            complete { HttpResponse(StatusCodes.NotFound) }
          }
        } ~
        /** Cluster is slave: /cluster/$name/is-master/$nodeId */
        path("is-slave" / Segment) { nodeId: String =>
          keeper.getClusterStatus(clusterName).map { case clusterStatus =>
            val isSlave = clusterStatus.slaves.exists(m => m.id == nodeId)
            complete { s"{'is-slave': '$isSlave'}" }
          }.getOrElse {
            complete { HttpResponse(StatusCodes.NotFound) }
          }
        }
      }
    }
  }

  def ExtractNodeIdFromHaproxyStatus(haproxyState: String): String = {
    val info = haproxyState.split(";").collect {
      case i if i.contains("=") =>
        val Array(key, value) = i.trim.split("=", 2)
        (key, value)
    }.toMap

    val Array(_, nodeId) = info("name").split("/", 2)
    nodeId
  }
}
