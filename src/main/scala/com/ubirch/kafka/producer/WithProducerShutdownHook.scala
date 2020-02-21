package com.ubirch.kafka.producer

import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps

trait WithProducerShutdownHook extends LazyLogging {
  def hookFunc(producerRunner: => ProducerRunner[_, _], timeout: FiniteDuration = 5 seconds): () => Future[Unit] = {
    () =>
      logger.info(s"Shutting down Producer[timeout=$timeout]...")
      if (Option(producerRunner).isDefined)
        Future.successful(producerRunner.close(timeout))
      else Future.unit

  }
}

object ProducerShutdownHook extends WithProducerShutdownHook
