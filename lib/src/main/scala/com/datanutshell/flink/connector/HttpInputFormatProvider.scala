package com.datanutshell.flink.connector

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.flink.api.common.io.statistics.BaseStatistics
import org.apache.flink.api.common.io.{DefaultInputSplitAssigner, InputFormat, RichInputFormat}
import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.io.{GenericInputSplit, InputSplit, InputSplitAssigner}
import org.apache.flink.formats.common.TimestampFormat
import org.apache.flink.formats.json.JsonRowDataDeserializationSchema
import org.apache.flink.metrics.MetricGroup
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup
import org.apache.flink.table.connector.source.InputFormatProvider
import org.apache.flink.table.data.RowData
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.logical.RowType
import org.apache.flink.util.UserCodeClassLoader
import org.slf4j.LoggerFactory

import java.net.{HttpURLConnection, URI}
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.time.Duration
import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success, Try}

class HttpRowDataInputFormatProvider(
    projectionDataType: DataType,
    url: String,
    jsonPointer: String,
    connectTimeoutSeconds: Int,
    readTimeoutSeconds: Int,
    maxRetries: Int,
    retryDelayMs: Int
) extends InputFormatProvider {

  override def createInputFormat(): InputFormat[RowData, _] = {
    new HttpRowDataInputFormat(
      projectionDataType,
      url,
      jsonPointer,
      connectTimeoutSeconds,
      readTimeoutSeconds,
      maxRetries,
      retryDelayMs
    )
  }

  override def isBounded: Boolean = true
}

class HttpRowDataInputFormat(
    projectionDataType: DataType,
    url: String,
    jsonPointer: String,
    connectTimeoutSeconds: Int,
    readTimeoutSeconds: Int,
    maxRetries: Int,
    retryDelayMs: Int
) extends RichInputFormat[RowData, InputSplit] {

  private val logger = LoggerFactory.getLogger(getClass)
  private var currentRow = 0
  private var maxRows = 0
  private var results: ArrayBuffer[RowData] = _
  @transient private var httpClient: HttpClient = _
  @transient private var objectMapper: ObjectMapper = _
  @transient private var deserializer: DeserializationSchema[RowData] = _

  override def configure(parameters: Configuration): Unit = {
    // Validate input parameters
    require(url.nonEmpty, "URL cannot be empty")
    require(connectTimeoutSeconds > 0, "Connect timeout must be positive")
    require(readTimeoutSeconds > 0, "Read timeout must be positive")
    require(maxRetries >= 0, "Max retries must be non-negative")
    require(retryDelayMs > 0, "Retry delay must be positive")
  }

  override def createInputSplits(minNumSplits: Int): Array[InputSplit] =
    Array(new GenericInputSplit(0, 1))

  private def fetchWithRetry(): HttpResponse[String] = {
    var lastException: Exception = null
    var attempt = 0

    while (attempt <= maxRetries) {
      try {
        val request = HttpRequest
          .newBuilder()
          .uri(URI.create(url))
          .timeout(Duration.ofSeconds(readTimeoutSeconds))
          .GET()
          .build()

        val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())

        if (response.statusCode() == HttpURLConnection.HTTP_OK) {
          return response
        }

        // For non-200 responses, throw an exception to trigger retry
        throw new RuntimeException(
          s"HTTP request failed with status ${response.statusCode()}. Response body: ${response.body()}"
        )
      } catch {
        case e: Exception =>
          lastException = e
          attempt += 1
          if (attempt <= maxRetries) {
            logger.warn(
              s"Attempt $attempt failed to fetch data from $url. Retrying in ${retryDelayMs}ms...",
              e
            )
            Thread.sleep(retryDelayMs)
          }
      }
    }

    throw new RuntimeException(
      s"Failed to fetch data from $url after $maxRetries retries",
      lastException
    )
  }

  private def fetchAndParseData(): Unit = {
    logger.info(s"Fetching data from $url")

    val response = fetchWithRetry()

    val jsonNode = Try(objectMapper.readTree(response.body())) match {
      case Success(node) => node
      case Failure(e) =>
        throw new RuntimeException("Failed to parse JSON response", e)
    }

    val resultNode = jsonPointer match {
      case "" => jsonNode
      case _  => jsonNode.at(jsonPointer)
    }

    if (resultNode.isMissingNode) {
      throw new RuntimeException(
        s"JSON Pointer '$jsonPointer' did not match any node in the response"
      )
    }

    results = ArrayBuffer.empty[RowData]
    resultNode match {
      case arrayNode if arrayNode.isArray =>
        maxRows = arrayNode.size()
        logger.info(s"Processing array with $maxRows elements")
        arrayNode.elements().forEachRemaining(node => results += deserializeNode(node))
      case singleNode =>
        maxRows = 1
        logger.info("Processing single JSON object")
        results += deserializeNode(singleNode)
    }

    logger.info(s"Successfully processed $maxRows records")
  }

  private def deserializeNode(node: JsonNode): RowData = {
    Try(deserializer.deserialize(objectMapper.writeValueAsBytes(node))) match {
      case Success(rowData) => rowData
      case Failure(e) =>
        throw new RuntimeException(
          s"Failed to deserialize JSON node: ${node.toPrettyString}",
          e
        )
    }
  }

  override def open(split: InputSplit): Unit = {
    currentRow = 0
    results = ArrayBuffer.empty

    httpClient = HttpClient
      .newBuilder()
      .connectTimeout(Duration.ofSeconds(connectTimeoutSeconds))
      .followRedirects(HttpClient.Redirect.NORMAL)
      .build()

    objectMapper = new ObjectMapper()

    val rowType = projectionDataType.getLogicalType.asInstanceOf[RowType]
    deserializer = new JsonRowDataDeserializationSchema(
      rowType,
      InternalTypeInfo.of(rowType),
      false,
      false,
      TimestampFormat.SQL
    )

    deserializer.open(new DeserializationSchema.InitializationContext {
      override def getMetricGroup: MetricGroup = new UnregisteredMetricsGroup()
      override def getUserCodeClassLoader: UserCodeClassLoader =
        Thread
          .currentThread()
          .getContextClassLoader
          .asInstanceOf[UserCodeClassLoader]
    })

    fetchAndParseData()
  }

  override def reachedEnd(): Boolean = currentRow >= maxRows

  override def nextRecord(reuse: RowData): RowData = {
    if (currentRow < maxRows) {
      val record = results(currentRow)
      currentRow += 1
      record
    } else {
      null
    }
  }

  override def close(): Unit = {
    // Clean up resources
    httpClient = null
    objectMapper = null
    deserializer = null
    results = null
  }

  override def getInputSplitAssigner(
      splits: Array[InputSplit]
  ): InputSplitAssigner =
    new DefaultInputSplitAssigner(splits)

  override def getStatistics(cachedStatistics: BaseStatistics): BaseStatistics =
    null

}
