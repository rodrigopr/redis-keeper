package br.com.jusbrasil.redis.keeper

import org.scalatest._
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import spray.client.pipelining._
import spray.http._
import akka.actor.ActorSystem

class RestApiTestSuite extends FlatSpec with KeeperBaseTestSuite {
  implicit val system = ActorSystem("test")
  import system.dispatcher
  val pipeline: HttpRequest => Future[HttpResponse] = sendReceive
  def restOperation(operation: String, port: Int, cluster: String = "cluster1") =
    Await.result(pipeline(Get(s"http://127.0.0.1:$port/cluster/$cluster/$operation")), 10.seconds)


  "KeeperRest" should "report cluster status" in {
    val conf = defaultConfig("keeper1", 46379)
    val conf2 = defaultConfig("keeper2", 46378)

    withKeepersOn(conf, conf2) { _ =>
      withRedisInstancesOn(7341) { _ =>
        Thread.sleep(15000)

        val expectedStatus = """{"master":{"host":"127.0.0.1","port":"7341"},"slaves":[]}"""
        assert(restOperation("status", 46379).entity.asString === expectedStatus)
        assert(restOperation("status", 46378).entity.asString === expectedStatus)

        withRedisInstancesOn(7342) { _ =>
          Thread.sleep(15000)

          val expectedStatus = """{"master":{"host":"127.0.0.1","port":"7341"},"slaves":[{"host":"127.0.0.1","port":"7342"}]}"""
          assert(restOperation("status", 46379).entity.asString === expectedStatus)
          assert(restOperation("status", 46378).entity.asString === expectedStatus)
        }
      }

      Thread.sleep(15000)

      val expectedStatus = """{"master":null,"slaves":[]}"""
      assert(restOperation("status", 46379).entity.asString === expectedStatus)
      assert(restOperation("status", 46378).entity.asString === expectedStatus)
    }
  }

  "KeeperRest" should "report cluster master" in {
    val conf = defaultConfig("keeper1", 46379)
    val conf2 = defaultConfig("keeper2", 46378)

    withKeepersOn(conf, conf2) { _ =>
      withRedisInstancesOn(7341) { _ =>
        Thread.sleep(15000)

        val expectedStatus = """{"host":"127.0.0.1","port":"7341"}"""
        assert(restOperation("master", 46379).entity.asString === expectedStatus)
        assert(restOperation("master", 46378).entity.asString === expectedStatus)

        withRedisInstancesOn(7342) { _ =>
          Thread.sleep(15000)

          assert(restOperation("master", 46379).entity.asString === expectedStatus)
          assert(restOperation("master", 46378).entity.asString === expectedStatus)
        }
      }

      Thread.sleep(15000)

      assert(restOperation("master", 46379).entity.asString === """null""")
      assert(restOperation("master", 46378).entity.asString === """null""")
    }
  }

  "KeeperRest" should "report if node is master" in {
    val conf = defaultConfig("keeper1", 46379)
    val conf2 = defaultConfig("keeper2", 46378)

    withKeepersOn(conf, conf2) { _ =>
      withRedisInstancesOn(7341) { _ =>
        Thread.sleep(15000)

        assert(restOperation("is-master/127.0.0.1:7341", 46379).entity.asString === """127.0.0.1:7341 is Master""")
        assert(restOperation("is-master/127.0.0.1:7341", 46378).entity.asString === """127.0.0.1:7341 is Master""")

        assert(restOperation("is-master/127.0.0.1:7342", 46379).status == StatusCodes.ServiceUnavailable)
        assert(restOperation("is-master/127.0.0.1:7342", 46378).status == StatusCodes.ServiceUnavailable)

        withRedisInstancesOn(7342) { _ =>
          Thread.sleep(15000)

          assert(restOperation("is-master/127.0.0.1:7341", 46379).entity.asString === """127.0.0.1:7341 is Master""")
          assert(restOperation("is-master/127.0.0.1:7341", 46378).entity.asString === """127.0.0.1:7341 is Master""")

          assert(restOperation("is-master/127.0.0.1:7342", 46379).status == StatusCodes.ServiceUnavailable)
          assert(restOperation("is-master/127.0.0.1:7342", 46378).status == StatusCodes.ServiceUnavailable)
        }
      }

      Thread.sleep(15000)

      assert(restOperation("is-master/127.0.0.1:7341", 46379).status == StatusCodes.ServiceUnavailable)
      assert(restOperation("is-master/127.0.0.1:7341", 46378).status == StatusCodes.ServiceUnavailable)

      assert(restOperation("is-master/127.0.0.1:7342", 46379).status == StatusCodes.ServiceUnavailable)
      assert(restOperation("is-master/127.0.0.1:7342", 46378).status == StatusCodes.ServiceUnavailable)
    }
  }

  "KeeperRest" should "report if node is slave" in {
    val conf = defaultConfig("keeper1", 46379)
    val conf2 = defaultConfig("keeper2", 46378)

    withKeepersOn(conf, conf2) { _ =>
      withRedisInstancesOn(7341) { _ =>
        Thread.sleep(15000)

        assert(restOperation("is-slave/127.0.0.1:7341", 46379).status === StatusCodes.ServiceUnavailable)
        assert(restOperation("is-slave/127.0.0.1:7341", 46378).status === StatusCodes.ServiceUnavailable)

        assert(restOperation("is-slave/127.0.0.1:7342", 46379).status === StatusCodes.ServiceUnavailable)
        assert(restOperation("is-slave/127.0.0.1:7342", 46378).status === StatusCodes.ServiceUnavailable)

        withRedisInstancesOn(7342) { _ =>
          Thread.sleep(15000)

          assert(restOperation("is-slave/127.0.0.1:7341", 46379).status == StatusCodes.ServiceUnavailable)
          assert(restOperation("is-slave/127.0.0.1:7341", 46378).status == StatusCodes.ServiceUnavailable)

          assert(restOperation("is-slave/127.0.0.1:7342", 46379).entity.asString === """127.0.0.1:7342 is Slave""")
          assert(restOperation("is-slave/127.0.0.1:7342", 46378).entity.asString === """127.0.0.1:7342 is Slave""")
        }
      }

      Thread.sleep(15000)

      assert(restOperation("is-slave/127.0.0.1:7341", 46379).status == StatusCodes.ServiceUnavailable)
      assert(restOperation("is-slave/127.0.0.1:7341", 46378).status == StatusCodes.ServiceUnavailable)

      assert(restOperation("is-slave/127.0.0.1:7342", 46379).status == StatusCodes.ServiceUnavailable)
      assert(restOperation("is-slave/127.0.0.1:7342", 46378).status == StatusCodes.ServiceUnavailable)
    }
  }
}

class KeeperTestSuite extends FlatSpec with KeeperBaseTestSuite {

  "RedisKeeper" should "detect offline nodes" in {

    withRedisInstancesOn(7341) { _ =>
      val conf = defaultConfig("keeper1", 46379)

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

  "RedisKeeper" should "work with multiple keepers" in {
    withRedisInstancesOn(7341) { _ =>

      val conf = defaultConfig("keeper1", 46379)
      val conf2 = defaultConfig("keeper2", 46378)
      val conf3 = defaultConfig("keeper3", 46377)

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

  "RedisKeeper" should "elect a new master in case of failure" in {
    withRedisInstancesOn(7341, 7343) { redisInstances =>

      val conf = defaultConfig("keeper1", 46379)
      val conf2 = defaultConfig("keeper2", 46378)
      val conf3 = defaultConfig("keeper3", 46377)

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

      val conf = defaultConfig("keeper1", 46379)
      val conf2 = defaultConfig("keeper2", 46378)

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

      val conf = defaultConfig("keeper1", 46379)
      val conf2 = defaultConfig("keeper2", 46378)
      val conf3 = defaultConfig("keeper3", 46377)
      val conf4 = defaultConfig("keeper4", 46376)

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
}
