package br.com.jusbrasil.redis.keeper

import scala.concurrent.duration.FiniteDuration

case class Tick(timeout: FiniteDuration)
case object FailoverTick
case object Ok
case class CheckFailover(leader: LeaderProcessor)
case object FailoverFinished
