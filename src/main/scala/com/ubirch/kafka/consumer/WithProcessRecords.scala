package com.ubirch.kafka.consumer

import java.util.Collections
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.{ CountDownLatch, TimeUnit }

import com.ubirch.kafka.util.Exceptions._
import com.ubirch.kafka.util.Implicits._
import org.apache.kafka.clients.consumer.{ OffsetAndMetadata, _ }
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.TimeoutException

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{ Failure, Success }

trait WithProcessRecords[K, V] {

  cr: ConsumerRunner[K, V] =>

  trait ProcessRecordsBase {

    val consumerRecords: ConsumerRecords[K, V]

    protected val batchCountDownSize: Int

    protected lazy val failed = new AtomicReference[Option[Throwable]](None)

    protected lazy val batchCountDown: CountDownLatch = new CountDownLatch(batchCountDownSize)

    def run(): Vector[Unit] = {
      start()
      aggregate()
      finish()
    }

    def processRecords(consumerRecords: Vector[ConsumerRecord[K, V]]) {
      val processing = process(consumerRecords)
      processing.onComplete {
        case Success(_) =>
          batchCountDown.countDown()
        case Failure(e) =>
          failed.set(Some(e))
          batchCountDown.countDown()
      }
    }

    def start(): Unit

    def aggregate(): Unit = {
      val aggRes = batchCountDown.await(maxTimeAggregationSeconds, TimeUnit.SECONDS)
      if (!aggRes) {
        logger.warn("Taking too much time aggregating ..")
      }
    }

    def commitFunc(): Vector[Unit]

    def finish(): Vector[Unit]

  }

  class ProcessRecordsOne(
      val currentPartitionIndex: Int,
      val currentPartition: TopicPartition,
      val allPartitions: Set[TopicPartition],
      val consumerRecords: ConsumerRecords[K, V]
  ) extends ProcessRecordsBase {

    protected lazy val partitionRecords: Vector[ConsumerRecord[K, V]] = consumerRecords.records(currentPartition).asScala.toVector

    protected lazy val partitionRecordsSize: Int = partitionRecords.size

    override protected val batchCountDownSize: Int = partitionRecordsSize

    def start() {
      if (getDelaySingleRecord == 0.millis && getDelayRecords == 0.millis) {
        partitionRecords.foreach(x => processRecords(Vector(x)))
      } else {
        partitionRecords.toIterator
          .delayOnNext(getDelaySingleRecord)
          .consumeWithFinalDelay(x => processRecords(Vector(x)))(getDelayRecords)
      }
    }

    def commitFunc(): Vector[Unit] = {

      try {
        val lastOffset = partitionRecords(partitionRecordsSize - 1).offset()
        consumer.commitSync(Collections.singletonMap(currentPartition, new OffsetAndMetadata(lastOffset + 1)))
        postCommitCallback.run(partitionRecordsSize)
      } catch {
        case e: TimeoutException =>
          throw CommitTimeoutException("Commit timed out", () => this.commitFunc(), e)
        case e: Throwable =>
          throw e
      }

    }

    def finish(): Vector[Unit] = {
      val error = failed.get()
      if (error.isDefined) {
        val initialOffsets = allPartitions.map { p =>
          (p, consumerRecords.records(p).asScala.headOption.map(_.offset()))
        }
        initialOffsets.drop(currentPartitionIndex).foreach {
          case (p, Some(of)) => consumer.seek(p, of)
          case (_, None) =>
        }
        failed.set(None)
        throw error.get
      } else {
        commitFunc()
      }

    }

  }

  class ProcessRecordsAll(val consumerRecords: ConsumerRecords[K, V]) extends ProcessRecordsBase {

    protected val batchCountDownSize: Int = 1

    def start() {
      processRecords(consumerRecords.iterator().asScala.toVector)
      if (getDelayRecords > 0.millis) {
        futureHelper.delay(getDelayRecords)(())
      }
    }

    def commitFuncUpgraded(): Vector[Unit] = {

      try {
        consumer.commitSync()
        postCommitCallback.run(consumerRecords.count())
      } catch {
        case e: TimeoutException =>
          throw CommitTimeoutException("Commit timed out", () => this.commitFuncUpgraded(), e)
        case e: Throwable =>
          throw e
      }

    }

    @deprecated("It makes commits slower. Do not use. Use commitFuncUpgraded. It will be removed soon.", "1.2.6")
    def commitFunc(): Vector[Unit] = {

      try {
        consumerRecords.partitions().asScala.foreach { p =>
          val partitionRecords = consumerRecords.records(p).asScala.toVector
          val partitionRecordsSize = partitionRecords.size
          val lastOffset = partitionRecords(partitionRecordsSize - 1).offset()
          consumer.commitSync(Collections.singletonMap(p, new OffsetAndMetadata(lastOffset + 1)))
        }
        postCommitCallback.run(consumerRecords.count())
      } catch {
        case e: TimeoutException =>
          throw CommitTimeoutException("Commit timed out", () => this.commitFunc(), e)
        case e: Throwable =>
          throw e
      }

    }

    def finish(): Vector[Unit] = {
      val error = failed.get()
      if (error.isDefined) {
        consumerRecords.partitions().asScala.foreach { p =>
          val offset = Option(consumer.committed(p)).map(_.offset()).getOrElse(0L)
          consumer.seek(p, offset)
        }
        failed.set(None)
        throw error.get
      } else {
        commitFuncUpgraded()
      }

    }

  }

  def allFactory(consumerRecords: ConsumerRecords[K, V]): ProcessRecordsAll = new ProcessRecordsAll(consumerRecords)

  def oneFactory(
      currentPartitionIndex: Int,
      currentPartition: TopicPartition,
      allPartitions: Set[TopicPartition],
      consumerRecords: ConsumerRecords[K, V]
  ): ProcessRecordsOne = {
    new ProcessRecordsOne(
      currentPartitionIndex,
      currentPartition,
      allPartitions,
      consumerRecords
    )
  }

  def maxTimeAggregationSeconds: Long

}
