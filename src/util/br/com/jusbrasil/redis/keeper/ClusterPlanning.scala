package br.com.jusbrasil.redis.keeper

import util.Random
import scala.collection.mutable

object ClusterPlanning extends App {
  def circular[T](xs: Stream[T]): Stream[T] = { lazy val knot: Stream[T] = xs #::: knot; knot }
  def cross[E1, E2](es1: Traversable[E1], es2: Traversable[E2]) = for (e1 <- es1; e2 <- es2) yield (e1, e2)
  val hosts = List(
    "esdoc1.jusbrasil.com.br",
    "esdoc2.jusbrasil.com.br",
    "esdoc3.jusbrasil.com",
    "esdoc4.jusbrasil.com",
    "esdoc5.jusbrasil.com.br",
    "esdoc6.jusbrasil.com.br",
    "esdoc7.jusbrasil.com",
    "esdoc8.jusbrasil.com.br",
    "esdoc9.jusbrasil.com",
    "esdoc10.jusbrasil.com"
  )

  val ports: List[Int] = List(
    6380, 6381, 6382, 6383,
    6480, 6481, 6482, 6483,
    6580, 6581, 6582, 6583
  )

  val instances = cross(hosts, ports)
  val instancesPerHost = instances.groupBy(g => g._1).map { case (host, hostInstances) =>
    host -> (mutable.HashSet() ++ hostInstances.map(i => i._2))
  }

  val clusters = 1.to(30).map { i =>
    val nodes = 1.to(3).foldLeft(List[(String, Int)]()) { case (list, _) =>
      val (hostName, ports) = Random.shuffle(instancesPerHost.toSeq)
        .filter { case (host, _) => !list.exists(p => p._1 == host) }
        .sortBy(_._2.size * -1)
        .head

      val port = ports.head
      ports.remove(port)

      (hostName, port) :: list
    }

    "Cluster-%d".format(i) -> nodes
  }

  clusters.foreach{ case (cluster, nodes) =>
    val nodesFormatted = nodes.map(p => "{'host': '%s', 'port': %d}".format(p._1, p._2)).mkString(",\n    ")
    println(s"""
      |{
      |  'name': '$cluster',
      |  'nodes': [
      |    $nodesFormatted
      |  ]
      |},""".stripMargin)
  }
}
