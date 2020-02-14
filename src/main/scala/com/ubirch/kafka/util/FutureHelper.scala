package com.ubirch.kafka.util

import java.util.concurrent.{ CountDownLatch, TimeUnit, Future => JavaFuture }

import monix.execution.Scheduler

import scala.concurrent.duration.{ Duration, FiniteDuration }
import scala.concurrent.{ Await, ExecutionContext, Future, blocking }

object FutureHelper {

  def withBlock[T](f: () => T)(implicit ec: ExecutionContext): Future[T] = {
    Future {
      blocking {
        f()
      }
    }
  }

  def fromJavaFuture[T](javaFuture: JavaFuture[T])(implicit ec: ExecutionContext): Future[T] = {
    withBlock(
      () => javaFuture.get(3000, TimeUnit.MILLISECONDS)
    )
  }

  def delay[T](duration: FiniteDuration)(t: => T)(implicit scheduler: Scheduler): T = {
    val countDown = new CountDownLatch(1)
    scheduler.scheduleOnce(duration)(countDown.countDown())
    countDown.await()
    t
  }

  def await[T](future: Future[T]): T = await(future, Duration.Inf)

  def await[T](future: Future[T], atMost: Duration): T = Await.result(future, atMost)

}
