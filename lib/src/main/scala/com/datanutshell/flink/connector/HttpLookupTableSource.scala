package com.datanutshell.flink.connector

import org.apache.flink.table.connector.Projection
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown
import org.apache.flink.table.connector.source.lookup.FullCachingLookupProvider
import org.apache.flink.table.connector.source.lookup.cache.LookupCache
import org.apache.flink.table.connector.source.lookup.cache.trigger.PeriodicCacheReloadTrigger.ScheduleMode
import org.apache.flink.table.connector.source.lookup.cache.trigger.PeriodicCacheReloadTrigger
import org.apache.flink.table.connector.source.LookupTableSource
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.logical.RowType

import java.time.Duration

class HttpLookupTableSource(
    val producedDataType: DataType,
    val url: String,
    val refreshCacheInterval: Duration,
    val method: String,
    val xpath: String,
    val connectTimeoutSeconds: Int,
    val readTimeoutSeconds: Int,
    val maxRetries: Int,
    val retryDelayMs: Int
) extends LookupTableSource
    with SupportsProjectionPushDown {

  var projectionDataType: DataType = producedDataType

  override def applyProjection(
      projectedFields: Array[Array[Int]],
      producedDataType: DataType
  ): Unit =
    projectionDataType = Projection.of(projectedFields).project(producedDataType)

  override def getLookupRuntimeProvider(
      context: LookupTableSource.LookupContext
  ): LookupTableSource.LookupRuntimeProvider = {
    FullCachingLookupProvider.of(
      new HttpRowDataInputFormatProvider(
        projectionDataType,
        url,
        xpath,
        connectTimeoutSeconds,
        readTimeoutSeconds,
        maxRetries,
        retryDelayMs
      ),
      new PeriodicCacheReloadTrigger(
        refreshCacheInterval,
        ScheduleMode.FIXED_DELAY
      )
    )
  }

  override def copy = new HttpLookupTableSource(
    producedDataType,
    url,
    refreshCacheInterval,
    method,
    xpath,
    connectTimeoutSeconds,
    readTimeoutSeconds,
    maxRetries,
    retryDelayMs
  )

  override def asSummaryString = "HttpLookupTableSource"

  override def supportsNestedProjection(): Boolean = true
}
