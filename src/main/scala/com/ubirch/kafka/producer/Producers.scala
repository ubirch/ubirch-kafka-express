package com.ubirch.kafka.producer

import org.apache.kafka.common.serialization.{ ByteArraySerializer, StringSerializer }

/**
  * Represents that represents a String Producer Factory
  */
abstract class StringProducer extends ProducerRunner[String, String]

/**
  * Represents the companion object of the String Producer
  */
object StringProducer {

  def empty: StringProducer = new StringProducer {}

  def apply(props: Map[String, AnyRef]): StringProducer = {
    require(props.nonEmpty, "Can't be empty")
    new StringProducer {}
      .withProps(props)
      .withKeySerializer(Some(new StringSerializer()))
      .withValueSerializer(Some(new StringSerializer()))

  }
}

/**
  * Represents a Bytes Producer
  */
abstract class BytesProducer extends ProducerRunner[String, Array[Byte]]

object BytesProducer {

  def empty: BytesProducer = new BytesProducer {}

  def apply(props: Map[String, AnyRef]): BytesProducer = {
    require(props.nonEmpty, "Can't be empty")
    new BytesProducer {}
      .withProps(props)
      .withKeySerializer(Some(new StringSerializer()))
      .withValueSerializer(Some(new ByteArraySerializer()))

  }
}
