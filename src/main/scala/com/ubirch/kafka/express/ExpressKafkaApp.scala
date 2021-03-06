package com.ubirch.kafka.express

import java.lang.{ Iterable => JIterable }
import java.nio.charset.StandardCharsets.UTF_8
import java.util.UUID
import java.util.concurrent.{ CountDownLatch, TimeUnit }
import com.typesafe.config.{ Config, ConfigFactory }
import com.typesafe.scalalogging.LazyLogging
import com.ubirch.kafka.consumer._
import com.ubirch.kafka.producer.{ ProducerBasicConfigs, ProducerRunner, WithProducerShutdownHook, WithSerializer }
import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.Observable
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.{ ProducerRecord, RecordMetadata }
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.internals.{ RecordHeader, RecordHeaders }

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.util.{ Failure, Success }

trait ExpressConsumer[K, V] extends ConsumerBasicConfigs with WithDeserializers[K, V] {

  def prefix: String

  def metricsSubNamespace: String

  def maxTimeAggregationSeconds: Long

  def controller: ConsumerRecordsController[K, V]

  def consumption: ConsumerRunner[K, V]

}

trait ExpressProducer[K, V] extends ProducerBasicConfigs with WithSerializer[K, V] {

  def production: ProducerRunner[K, V]

  def send(pr: ProducerRecord[K, V]): Future[RecordMetadata] = production.send(pr)

  def send(topic: String, value: V): Future[RecordMetadata] = send(new ProducerRecord[K, V](topic, value))

  def send(topic: String, value: V, headers: (String, String)*): Future[RecordMetadata] = {
    val headersIterable: JIterable[Header] = headers
      .map(p => new RecordHeader(p._1, p._2.getBytes(UTF_8)): Header).asJava
    val pm: ProducerRecord[K, V] = new ProducerRecord(topic, null, null, null.asInstanceOf[K], value, new RecordHeaders(headersIterable))
    send(pm)
  }

}

trait WithShutdownHook extends WithConsumerShutdownHook with WithProducerShutdownHook {
  ek: ExpressKafka[_, _, _] =>

  Runtime.getRuntime.addShutdownHook(new Thread() {
    override def run(): Unit = {
      val countDownLatch = new CountDownLatch(1)
      (for {
        _ <- hookFunc(consumerGracefulTimeout, consumption)()
        _ <- hookFunc(production)()
      } yield ())
        .onComplete {
          case Success(_) => countDownLatch.countDown()
          case Failure(e) =>
            logger.error("Error running jvm hook={}", e.getMessage)
            countDownLatch.countDown()
        }

      val res = countDownLatch.await(5000, TimeUnit.SECONDS) //Waiting 5 secs
      if (!res) logger.warn("Taking too much time shutting down :(  ..")
      else logger.info("Bye bye, see you later...")
    }
  })
}

trait WithMain {
  ek: ExpressKafkaApp[_, _, _] =>

  def main(args: Array[String]): Unit = {
    start()
    val cd = new CountDownLatch(1)
    cd.await()
  }

}

object ConfigBase {
  lazy val conf: Config = ConfigFactory.load()
}

trait ConfigBase {
  def conf: Config = ConfigBase.conf
}

trait ExpressKafka[K, V, R] extends ExpressConsumer[K, V] with ExpressProducer[K, V] {
  thiz =>

  implicit def scheduler: Scheduler

  lazy val controller: ConsumerRecordsController[K, V] = new ConsumerRecordsController[K, V] {

    def simpleProcessResult(consumerRecord: Vector[ConsumerRecord[K, V]]): ProcessResult[K, V] = new ProcessResult[K, V] {
      override lazy val id: UUID = UUID.randomUUID()
      override lazy val consumerRecords: Vector[ConsumerRecord[K, V]] = consumerRecord
    }

    override type A = ProcessResult[K, V]
    override def process(consumerRecords: Vector[ConsumerRecord[K, V]]): Future[ProcessResult[K, V]] = {
      thiz.process.invoke(consumerRecords).map(_ => simpleProcessResult(consumerRecords))
    }
  }

  lazy val production: ProducerRunner[K, V] = ProducerRunner(producerConfigs, Some(keySerializer), Some(valueSerializer))

  lazy val consumption: ConsumerRunner[K, V] = {
    val impl = ConsumerRunner.emptyWithMetrics[K, V](prefix, metricsSubNamespace)
    impl.setUseAutoCommit(false)
    impl.setTopics(consumerTopics)
    impl.setProps(consumerConfigs)
    impl.setKeyDeserializer(Some(keyDeserializer))
    impl.setValueDeserializer(Some(valueDeserializer))
    impl.setConsumerRecordsController(Some(controller))
    impl.setConsumptionStrategy(All)
    impl.setMaxTimeAggregationSeconds(maxTimeAggregationSeconds)
    impl
  }

  trait Process {
    def invoke(consumerRecords: Vector[ConsumerRecord[K, V]]): Future[R]
  }

  object Process {
    def apply(p: Vector[ConsumerRecord[K, V]] => R): Process = (consumerRecords: Vector[ConsumerRecord[K, V]]) => Future(p(consumerRecords))
    def sync(p: Vector[ConsumerRecord[K, V]] => R): Process = apply(p)
    def async(p: Vector[ConsumerRecord[K, V]] => Future[R]): Process = (consumerRecords: Vector[ConsumerRecord[K, V]]) => p(consumerRecords)
    def task(p: Vector[ConsumerRecord[K, V]] => Task[R]): Process = (consumerRecords: Vector[ConsumerRecord[K, V]]) => p(consumerRecords).runToFuture
    def obs(p: Observable[ConsumerRecord[K, V]] => Task[R]): Process = (consumerRecords: Vector[ConsumerRecord[K, V]]) => p(Observable.fromIterable(consumerRecords)).runToFuture
  }

  def process: Process

  def start(): Unit = consumption.startPolling()

}

trait ExpressKafkaApp[K, V, R]
  extends ExpressKafka[K, V, R]
  with WithShutdownHook
  with WithMain
  with ConfigBase
  with LazyLogging

