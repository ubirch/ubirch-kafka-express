package com.ubirch.kafka.metrics

import java.net.{ BindException, InetSocketAddress }

import com.typesafe.scalalogging.LazyLogging
import io.prometheus.client.exporter.HTTPServer
import io.prometheus.client.hotspot.{ BufferPoolsExports, ClassLoadingExports, GarbageCollectorExports, MemoryAllocationExports, MemoryPoolsExports, StandardExports, ThreadExports, VersionInfoExports }
import io.prometheus.client.CollectorRegistry
import io.prometheus.jmx.JmxCollector
import javax.management.MalformedObjectNameException

import scala.annotation.tailrec

object PrometheusMetricsHelper extends LazyLogging {

  final private val maxAttempts = 3

  def create(port: Int): HTTPServer = {
    val customCollectionRegistry = createCustomCollectorRegistry

    @tailrec
    def go(attempts: Int, port: Int): HTTPServer = {
      try {
        new HTTPServer(new InetSocketAddress(port), customCollectionRegistry)
      } catch {
        case e: BindException =>
          val newPort = port + new scala.util.Random().nextInt(50)
          logger.debug("Attempt[{}], Port[{}] is busy, trying Port[{}]", attempts, port, newPort)
          if (attempts == maxAttempts) {
            throw e
          } else {
            go(attempts + 1, newPort)
          }
      }
    }

    val server = go(0, port)
    logger.debug(s"You can visit http://localhost:${server.getPort}/ to check the metrics.")
    server
  }

  /**
    * In order to get JMX metrics exported to Prometheus, using the default CollectorRegistry with
    * DefaultExports.initialize() will not work. Hence, this function creates a special collectorRegistry that collects
    * the Jmx metrics.
    * @return A custom CollectorRegistry containing Jmx values
    */
  private def createCustomCollectorRegistry = {
    val collectorRegistry = new CollectorRegistry
    try
      new JmxCollector("{}").register(collectorRegistry)
    catch {
      case ex: MalformedObjectNameException =>
        logger.error("Error starting Prometheus Jmx Collector. Prometheus will start but without MBeans and" +
          "some Jmx metrics.", ex)
    }
    // Add the default exports from io.prometheus.client.hotspot.DefaultExports
    new StandardExports().register(collectorRegistry)
    new MemoryPoolsExports().register(collectorRegistry)
    new MemoryAllocationExports().register()
    new BufferPoolsExports().register(collectorRegistry)
    new GarbageCollectorExports().register(collectorRegistry)
    new ThreadExports().register(collectorRegistry)
    new ClassLoadingExports().register(collectorRegistry)
    new VersionInfoExports().register(collectorRegistry)
    collectorRegistry
  }

}
