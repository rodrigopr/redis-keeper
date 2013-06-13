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
        // Cluster status: /cluster/$name/status
        path("status") {
          keeper.getClusterStatus(clusterName).map{ clusterStatus =>
            complete { clusterStatus.asJson.nospaces }
          }.getOrElse {
            complete { HttpResponse(StatusCodes.NotFound) }
          }
        } ~
        // Cluster master: /cluster/$name/master
        path("master") {
          keeper.getClusterStatus(clusterName).map { clusterStatus =>
            complete { clusterStatus.master.asJson.nospaces }
          }.getOrElse {
            complete { HttpResponse(StatusCodes.NotFound) }
          }
        } ~
        // Node is master: /cluster/$name/is-master/$nodeId
        path("is-master" / Segment) { nodeId: String =>
          keeper.getClusterStatus(clusterName).map { clusterStatus =>
            clusterStatus.master.find(m => m.id == nodeId).map { _ =>
              complete { nodeId + " is Master" }
            }.getOrElse {
              complete { HttpResponse(StatusCodes.ServiceUnavailable) }
            }
          }.getOrElse {
            complete { HttpResponse(StatusCodes.NotFound) }
          }
        } ~
        // Cluster is slave: /cluster/$name/is-master/$nodeId
        path("is-slave" / Segment) { nodeId: String =>
          keeper.getClusterStatus(clusterName).map { case clusterStatus =>
            clusterStatus.slaves.find(n => n.id == nodeId).map { _ =>
              complete { nodeId + " is Slave" }
            }.getOrElse {
              complete { HttpResponse(StatusCodes.ServiceUnavailable) }
            }
          }.getOrElse {
            complete { HttpResponse(StatusCodes.NotFound) }
          }
        }
      }
    }
  }
}
