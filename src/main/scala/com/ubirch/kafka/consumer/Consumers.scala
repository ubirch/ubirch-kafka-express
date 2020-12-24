package com.ubirch.kafka.consumer

import monix.execution.Scheduler
import org.apache.kafka.clients.consumer.ConsumerRecord

import scala.concurrent.Future

/**
  * Represents a Basic String Consumer
  * @param scheduler Represents the execution scheduler.
  */
abstract class StringConsumer(implicit scheduler: Scheduler) extends ConsumerRunner[String, String](ConsumerRunner.name)

/**
  * Contains useful helpers for a StringConsumer
  */
object StringConsumer {

  def empty(implicit scheduler: Scheduler): StringConsumer = new StringConsumer() {}

  def emptyWithMetrics(prefix: String, metricsSubNamespace: String)(implicit scheduler: Scheduler): StringConsumer = new StringConsumer() with WithMetrics {

    override def prefixNamespace: String = prefix

    override def metricsSubNamespaceLabel: String = metricsSubNamespace
  }

  def fBased(f: Vector[ConsumerRecord[String, String]] => Future[ProcessResult[String, String]])(implicit scheduler: Scheduler): StringConsumer = {
    new StringConsumer() {
      override def process(consumerRecords: Vector[ConsumerRecord[String, String]]): Future[ProcessResult[String, String]] = {
        f(consumerRecords)
      }
    }
  }

  def controllerBased(controller: ConsumerRecordsController[String, String])(implicit scheduler: Scheduler): StringConsumer = {
    val consumer = empty
    consumer.setConsumerRecordsController(Some(controller))
    consumer
  }

}

/**
  * Represents a Basic Bytes Consumer
  * @param scheduler Represents the execution scheduler.
  */
abstract class BytesConsumer(implicit scheduler: Scheduler) extends ConsumerRunner[String, Array[Byte]](ConsumerRunner.name)

/**
  * Contains useful helpers for a  BytesConsumer
  */
object BytesConsumer {

  def empty(implicit scheduler: Scheduler): BytesConsumer = new BytesConsumer() {}

  def emptyWithMetrics(prefix: String, metricsSubNamespace: String)(implicit scheduler: Scheduler): BytesConsumer = new BytesConsumer() with WithMetrics {
    override def prefixNamespace: String = prefix
    override def metricsSubNamespaceLabel: String = metricsSubNamespace
  }

  def fBased(f: Vector[ConsumerRecord[String, Array[Byte]]] => Future[ProcessResult[String, Array[Byte]]])(implicit scheduler: Scheduler): BytesConsumer = {
    new BytesConsumer() {
      override def process(consumerRecords: Vector[ConsumerRecord[String, Array[Byte]]]): Future[ProcessResult[String, Array[Byte]]] = {
        f(consumerRecords)
      }
    }
  }

  def controllerBased(controller: ConsumerRecordsController[String, Array[Byte]])(implicit scheduler: Scheduler): BytesConsumer = {
    val consumer = empty
    consumer.setConsumerRecordsController(Some(controller))
    consumer
  }

}

