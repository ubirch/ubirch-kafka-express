package com.ubirch.kafka.util

import java.nio.charset.StandardCharsets.UTF_8

import monix.execution.Scheduler
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.joda.time.{ Duration, Instant }

import scala.collection.JavaConverters._
import scala.collection.breakOut
import scala.concurrent.duration.FiniteDuration
import scala.language.implicitConversions

/**
  * It is an enriched iterator
  * @param iterator Represents the iterator that gets enriched
  * @tparam A Represents the type of the elems found in the iterator
  */
case class EnrichedIterator[A](iterator: Iterator[A]) {

  def delayOnNext(duration: FiniteDuration)(implicit scheduler: Scheduler): Iterator[A] = iterator.map { x =>
    FutureHelper.delay(duration)(x)
  }

  def consumeWithFinalDelay[U](f: A => U)(duration: FiniteDuration)(implicit scheduler: Scheduler): Unit = {
    while (iterator.hasNext) f(iterator.next())
    FutureHelper.delay(duration)(())
  }

}

/**
  * It is an enriched instant
  * @param instant Represents the instant that gets enriched
  */
case class EnrichedInstant(instant: Instant) {

  def duration(other: Instant) = new Duration(instant, other)

  def millisBetween(other: Instant): Long = duration(other).getMillis

  def secondsBetween(other: Instant): Long = duration(other).getStandardSeconds

}

case class EnrichedConsumerRecord[K, V](cr: ConsumerRecord[K, V]) extends AnyVal {

  def findHeader(key: String): Option[String] = {
    cr.headers()
      .asScala
      .find(h => h.key().toLowerCase == key.toLowerCase)
      .map(h => new String(h.value(), UTF_8))
  }

  def existsHeader(key: String): Boolean = findHeader(key).isDefined

  def headersScala: Map[String, String] = cr.headers()
    .asScala
    .map(h => h.key() -> new String(h.value(), UTF_8))(breakOut)

}

/**
  * Util that contains the implicits to create enriched values.
  */
object Implicits {

  implicit def enrichedInstant(instant: Instant): EnrichedInstant = EnrichedInstant(instant)

  implicit def enrichedIterator[T](iterator: Iterator[T]): EnrichedIterator[T] = EnrichedIterator[T](iterator)

  implicit def enrichedConsumerRecord[K, V](cr: ConsumerRecord[K, V]): EnrichedConsumerRecord[K, V] = EnrichedConsumerRecord[K, V](cr)

}
