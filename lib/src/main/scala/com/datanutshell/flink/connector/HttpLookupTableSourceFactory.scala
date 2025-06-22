package com.datanutshell.flink.connector

import org.apache.flink.configuration.{
  ConfigOption,
  ConfigOptions,
  ReadableConfig
}
import org.apache.flink.table.connector.source.DynamicTableSource
import org.apache.flink.table.connector.source.lookup.LookupOptions
import org.apache.flink.table.connector.source.lookup.cache.{
  DefaultLookupCache,
  LookupCache
}
import org.apache.flink.table.factories.{
  DynamicTableFactory,
  DynamicTableSourceFactory,
  FactoryUtil
}
import org.apache.flink.table.api.ValidationException

import java.time.Duration
import java.util
import java.util.Collections

// Factory for creating HttpLookupTableSource
// Allows to fetch data from a URL and extract data using a JSON pointer for lookup enrichment with full caching
// Supports the following options:
// - url: The URL to fetch data from (required)
// - xpath: The JSON pointer expression to extract data from the response (required)
// - cache.refresh-interval: The interval to refresh the cache (default: PT5M)
// - method: The HTTP method to use (default: GET)
// - connect.timeout.seconds: Connection timeout in seconds (default: 10)
// - read.timeout.seconds: Read timeout in seconds (default: 30)
// - max.retries: Maximum number of retries for failed requests (default: 3)
// - retry.delay.ms: Delay between retries in milliseconds (default: 1000)
class HttpLookupTableSourceFactory extends DynamicTableSourceFactory {

  private val URL = ConfigOptions.key("url").stringType().noDefaultValue()
  private val XPATH = ConfigOptions.key("xpath").stringType().noDefaultValue()
  private val CACHE_REFRESH_INTERVAL = ConfigOptions
    .key("cache.refresh-interval")
    .stringType()
    .defaultValue("PT5M")
  private val METHOD =
    ConfigOptions.key("method").stringType().defaultValue("GET")
  private val CONNECT_TIMEOUT = ConfigOptions
    .key("connect.timeout.seconds")
    .intType()
    .defaultValue(10)
  private val READ_TIMEOUT = ConfigOptions
    .key("read.timeout.seconds")
    .intType()
    .defaultValue(30)
  private val MAX_RETRIES = ConfigOptions
    .key("max.retries")
    .intType()
    .defaultValue(3)
  private val RETRY_DELAY = ConfigOptions
    .key("retry.delay.ms")
    .intType()
    .defaultValue(1000)

  override def createDynamicTableSource(
      context: DynamicTableFactory.Context
  ): DynamicTableSource = {
    val helper = FactoryUtil.createTableFactoryHelper(this, context)
    helper.validate()

    val options = helper.getOptions
    val url = options.get(URL)
    val xpath = options.get(XPATH)
    
    // Validate refresh interval format
    val refreshIntervalStr = options.get(CACHE_REFRESH_INTERVAL)
    val refreshInterval = try {
      Duration.parse(refreshIntervalStr)
    } catch {
      case e: Exception =>
        throw new ValidationException(
          s"Invalid format for cache.refresh-interval: '$refreshIntervalStr'. " +
          "Expected format is ISO-8601 duration (e.g., PT5M for 5 minutes)."
        )
    }
    
    // Validate timeout values
    val connectTimeout = options.get(CONNECT_TIMEOUT)
    if (connectTimeout <= 0) {
      throw new ValidationException(
        s"Invalid value for connect.timeout.seconds: $connectTimeout. Must be positive."
      )
    }
    
    val readTimeout = options.get(READ_TIMEOUT)
    if (readTimeout <= 0) {
      throw new ValidationException(
        s"Invalid value for read.timeout.seconds: $readTimeout. Must be positive."
      )
    }
    
    val maxRetries = options.get(MAX_RETRIES)
    if (maxRetries < 0) {
      throw new ValidationException(
        s"Invalid value for max.retries: $maxRetries. Must be non-negative."
      )
    }
    
    val retryDelay = options.get(RETRY_DELAY)
    if (retryDelay <= 0) {
      throw new ValidationException(
        s"Invalid value for retry.delay.ms: $retryDelay. Must be positive."
      )
    }

    val method = options.get(METHOD)

    new HttpLookupTableSource(
      context.getPhysicalRowDataType,
      url,
      refreshInterval,
      method,
      xpath,
      connectTimeout,
      readTimeout,
      maxRetries,
      retryDelay
    )
  }

  override def factoryIdentifier(): String = "http-lookup-full-cache"

  override def requiredOptions(): util.Set[ConfigOption[_]] =
    Collections.singleton(URL)

  override def optionalOptions(): util.Set[ConfigOption[_]] =
    util.Set.of(
      XPATH,
      METHOD,
      CACHE_REFRESH_INTERVAL,
      CONNECT_TIMEOUT,
      READ_TIMEOUT,
      MAX_RETRIES,
      RETRY_DELAY
    )
}
