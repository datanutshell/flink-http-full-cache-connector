package com.datanutshell.flink.connector

import org.apache.flink.configuration.{ConfigOption, ConfigOptions, ReadableConfig}
import org.apache.flink.table.api.{DataTypes, Schema, ValidationException}
import org.apache.flink.table.catalog.{CatalogBaseTable, CatalogTable, ObjectIdentifier, ResolvedCatalogTable}
import org.apache.flink.table.connector.source.DynamicTableSource
import org.apache.flink.table.factories.DynamicTableFactory
import org.apache.flink.table.factories.FactoryUtil
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{mock, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.Duration
import java.util

class HttpLookupTableSourceFactoryTest extends AnyFlatSpec with Matchers {

  "HttpLookupTableSourceFactory" should "create table source with required options" in {
    val factory = new HttpLookupTableSourceFactory
    val context = createContext(
      Map(
        "url" -> "https://api.example.com/data",
        "xpath" -> "/data"
      )
    )

    val source = factory.createDynamicTableSource(context)
    source shouldBe a[HttpLookupTableSource]
  }

  it should "create table source with all optional options" in {
    val factory = new HttpLookupTableSourceFactory
    val context = createContext(
      Map(
        "url" -> "https://api.example.com/data",
        "xpath" -> "/data",
        "cache.refresh-interval" -> "PT10M",
        "method" -> "POST",
        "connect.timeout.seconds" -> "20",
        "read.timeout.seconds" -> "40",
        "max.retries" -> "5",
        "retry.delay.ms" -> "2000"
      )
    )

    val source = factory.createDynamicTableSource(context)
    source shouldBe a[HttpLookupTableSource]
    
    val httpSource = source.asInstanceOf[HttpLookupTableSource]
    httpSource.url shouldBe "https://api.example.com/data"
    httpSource.xpath shouldBe "/data"
    httpSource.refreshCacheInterval shouldBe Duration.parse("PT10M")
    httpSource.method shouldBe "POST"
    httpSource.connectTimeoutSeconds shouldBe 20
    httpSource.readTimeoutSeconds shouldBe 40
    httpSource.maxRetries shouldBe 5
    httpSource.retryDelayMs shouldBe 2000
  }

  it should "use default values for optional options" in {
    val factory = new HttpLookupTableSourceFactory
    val context = createContext(
      Map(
        "url" -> "https://api.example.com/data",
        "xpath" -> "/data"
      )
    )

    val source = factory.createDynamicTableSource(context)
    val httpSource = source.asInstanceOf[HttpLookupTableSource]
    
    httpSource.refreshCacheInterval shouldBe Duration.parse("PT5M")
    httpSource.method shouldBe "GET"
    httpSource.connectTimeoutSeconds shouldBe 10
    httpSource.readTimeoutSeconds shouldBe 30
    httpSource.maxRetries shouldBe 3
    httpSource.retryDelayMs shouldBe 1000
  }

  it should "throw exception when required url option is missing" in {
    val factory = new HttpLookupTableSourceFactory
    val context = createContext(
      Map(
        "xpath" -> "/data"
      )
    )

    val exception = intercept[ValidationException] {
      factory.createDynamicTableSource(context)
    }
    
    exception.getMessage should include("url")
  }

  it should "throw exception for invalid refresh interval format" in {
    val factory = new HttpLookupTableSourceFactory
    val context = createContext(
      Map(
        "url" -> "https://api.example.com/data",
        "xpath" -> "/data",
        "cache.refresh-interval" -> "invalid"
      )
    )

    val exception = intercept[ValidationException] {
      factory.createDynamicTableSource(context)
    }
    
    exception.getMessage should include("cache.refresh-interval")
  }

  it should "throw exception for invalid timeout values" in {
    val factory = new HttpLookupTableSourceFactory
    val context = createContext(
      Map(
        "url" -> "https://api.example.com/data",
        "xpath" -> "/data",
        "connect.timeout.seconds" -> "-1"
      )
    )

    val exception = intercept[ValidationException] {
      factory.createDynamicTableSource(context)
    }
    
    exception.getMessage should include("connect.timeout.seconds")
  }

  it should "have correct factory identifier" in {
    val factory = new HttpLookupTableSourceFactory
    factory.factoryIdentifier() shouldBe "http-lookup-full-cache"
  }

  it should "have correct required options" in {
    val factory = new HttpLookupTableSourceFactory
    val requiredOptions = factory.requiredOptions()
    
    requiredOptions.size() shouldBe 1
    requiredOptions.contains(ConfigOptions.key("url").stringType().noDefaultValue()) shouldBe true
  }

  private def createContext(options: Map[String, String]): DynamicTableFactory.Context = {
    // Create mocks
    val context = mock(classOf[DynamicTableFactory.Context])
    val catalogTable = mock(classOf[ResolvedCatalogTable])
    val readableConfig = mock(classOf[ReadableConfig])
    
    // Setup catalog table mock
    when(catalogTable.getUnresolvedSchema).thenReturn(Schema.newBuilder().build())
    when(catalogTable.getPartitionKeys).thenReturn(util.Collections.emptyList[String]())
    when(catalogTable.getComment).thenAnswer(_ => util.Optional.empty[String]())
    when(catalogTable.getDescription).thenAnswer(_ => util.Optional.empty[String]())
    when(catalogTable.getTableKind).thenReturn(CatalogBaseTable.TableKind.TABLE)
    
    // Setup options map
    val optionsMap = new util.HashMap[String, String]()
    options.foreach { case (k, v) => optionsMap.put(k, v) }
    when(catalogTable.getOptions).thenReturn(optionsMap)
    
    // Setup readable config mock
    when(readableConfig.get(any[ConfigOption[Any]])).thenAnswer(invocation => {
      val option = invocation.getArgument[ConfigOption[Any]](0)
      options.get(option.key()) match {
        case Some(value) => value
        case None => option.defaultValue()
      }
    })
    
    when(readableConfig.getOptional(any[ConfigOption[Any]])).thenAnswer(invocation => {
      val option = invocation.getArgument[ConfigOption[Any]](0)
      options.get(option.key()) match {
        case Some(value) => util.Optional.of(value)
        case None => util.Optional.empty()
      }
    })
    
    when(readableConfig.toMap).thenReturn(optionsMap)
    
    // Setup context mock
    when(context.getCatalogTable).thenReturn(catalogTable)
    when(context.getObjectIdentifier).thenReturn(
      ObjectIdentifier.of("default_catalog", "default_database", "test_table")
    )
    when(context.isTemporary).thenReturn(false)
    when(context.getConfiguration).thenReturn(readableConfig)
    when(context.getClassLoader).thenReturn(getClass.getClassLoader)
    when(context.getPhysicalRowDataType).thenReturn(
      DataTypes.ROW(
        DataTypes.FIELD("id", DataTypes.INT()),
        DataTypes.FIELD("name", DataTypes.STRING())
      )
    )
    
    context
  }
} 