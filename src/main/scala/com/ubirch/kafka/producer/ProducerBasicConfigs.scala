package com.ubirch.kafka.producer

import com.ubirch.kafka.util.ConfigProperties
import org.apache.kafka.common.serialization.Serializer

trait WithSerializer[K, V] {
  def keySerializer: Serializer[K]
  def valueSerializer: Serializer[V]
}

trait ProducerBasicConfigs {

  def producerBootstrapServers: String

  def lingerMs: Int

  def producerConfigs: ConfigProperties = Configs(producerBootstrapServers, lingerMs = lingerMs)

}
