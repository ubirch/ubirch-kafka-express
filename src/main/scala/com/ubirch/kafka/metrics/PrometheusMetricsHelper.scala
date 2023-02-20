package com.ubirch.kafka.metrics

import java.net.{ BindException, InetSocketAddress }

import com.typesafe.scalalogging.LazyLogging
import io.prometheus.client.exporter.HTTPServer
import io.prometheus.client.hotspot.{ BufferPoolsExports, ClassLoadingExports, DefaultExports, GarbageCollectorExports, MemoryAllocationExports, MemoryPoolsExports, StandardExports, ThreadExports, VersionInfoExports }
import io.prometheus.client.CollectorRegistry
import io.prometheus.jmx.JmxCollector
import javax.management.MalformedObjectNameException

import scala.annotation.tailrec

object PrometheusMetricsHelper extends LazyLogging {

  /**
    * Simple and default connection backed up by the default registry
    * @param port the port number to start the server on
    * @param maxAttempts the number of retries for when there is a Connection problem.
    *                    If not provided, 3 attempts will be performed
    * @return the Prometheus Http Server
    */
  def default(port: Int, maxAttempts: Int = 3): HTTPServer = create(port, defaultCollectorRegistry, maxAttempts)

  /**
    * Connection backed up by JMX registry. It will exports JMX values as Prometheus metrics.
    * @param port the port number to start the server on
    * @param maxAttempts the number of retries for when there is a Connection problem.
    *                    If not provided, 3 attempts will be performed
    * @return the Prometheus Http Server
    */
  def defaultWithJXM(port: Int, maxAttempts: Int = 3): HTTPServer = create(port, createCustomCollectorRegistry, maxAttempts)

  /**
    * Represents a builder for the http server for prometheus
    * @param port the port number to start the server on
    * @param collectorRegistry the registry for the metrics
    * @param maxAttempts the number of retries for when there is a Connection problem.
    * @return the Prometheus Http Server
    */
  private final def create(port: Int, collectorRegistry: CollectorRegistry, maxAttempts: Int): HTTPServer = {

    @tailrec
    def go(attempts: Int, port: Int): HTTPServer = {
      try {
        new HTTPServer(new InetSocketAddress(port), collectorRegistry)
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
    * Represents a simple and default collector registry and its initialization
    * @return A default CollectorRegistry
    */
  private def defaultCollectorRegistry = {
    try {
      DefaultExports.initialize()
      CollectorRegistry.defaultRegistry
    } catch {
      case ex: Exception =>
        logger.error("Error starting Default Prometheus Collector", ex)
        throw ex
    }
  }

  /**
    * In order to get JMX metrics exported to Prometheus, using the default CollectorRegistry with
    * DefaultExports.initialize() will not work. Hence, this function creates a special collectorRegistry that collects
    * the Jmx metrics.
    * @return A custom CollectorRegistry containing Jmx values
    */
  private def createCustomCollectorRegistry = {
    try {
      val collectorRegistry = new CollectorRegistry
      new JmxCollector("{}").register(collectorRegistry)
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
    } catch {
      case ex: MalformedObjectNameException =>
        logger.error("Error starting Prometheus Jmx Collector. Prometheus will start but without MBeans and some Jmx metrics.", ex)
        throw ex
      case ex: Exception =>
        logger.error("Error starting Prometheus Jmx Collector.", ex)
        throw ex
    }
  }

}
