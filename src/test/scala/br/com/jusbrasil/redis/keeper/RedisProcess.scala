package br.com.jusbrasil.redis.keeper

import scala.collection.mutable
import scala.sys.process._
import scala.util.Try
import org.apache.log4j.Logger

class RedisProcess(val port: Int) {
  private val logger = Logger.getLogger(classOf[RedisProcess])
  private val redisPath = System.getProperty("redis-server-bin", "/usr/local/bin/redis-server")
  val outputLog = new mutable.ListBuffer[String]
  val errorLog = new mutable.ListBuffer[String]

  private val configFile = s"src/test/conf/redis_$port.conf"

  private var running = false
  private var process: scala.sys.process.Process = _
  start()

  def start() {
    if(!running) {
      outputLog.clear()
      errorLog.clear()
      process =
        Seq("sh", "-c", "%s %s".format(redisPath, configFile)).run(new ProcessLogger {
          def buffer[T](f: => T): T = f

          def out(s: => String) {
            outputLog.+=:(s)
          }

          def err(s: => String) {
            errorLog.+=:(s)
            logger.error("[Redis %s] %s".format(port, s))
          }
        })

      running = true
    }
  }

  def stopProcess() {
    Try {
      RedisNode("127.0.0.1", port).withConnection { conn =>
        conn.shutdown
      }
      process.exitValue()
    } recover {
      case _ => process.destroy()
    }
    running = false
  }

  override def toString = s"Redis-$port"
}
