package br.com.jusbrasil.redis.keeper

import org.scalatest._

class KeeperTestSuite extends FlatSpec with KeeperBaseTestSuite {
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
    val conf = KeeperConfig(keeperId, 1, 5, 2, List(zkQuorum), "/rediskeeper", List(clusterCopy))
    conf
  }
}
