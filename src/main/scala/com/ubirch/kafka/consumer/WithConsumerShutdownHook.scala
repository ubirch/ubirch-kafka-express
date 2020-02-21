package com.ubirch.kafka.consumer

import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.Future

trait WithConsumerShutdownHook extends LazyLogging {
  def hookFunc(gracefulTimeout: => Int, consumerRunner: => ConsumerRunner[_, _]): () => Future[Unit] = {
    () =>
      logger.info(s"Shutting down Consumer[timeout=$gracefulTimeout secs name=${consumerRunner.getName}] ...")
      if (Option(consumerRunner).isDefined)
        Future.successful(consumerRunner.shutdown(gracefulTimeout, java.util.concurrent.TimeUnit.SECONDS))
      else Future.unit
  }
}

object ConsumerShutdownHook extends WithConsumerShutdownHook
