package com.datanutshell.flink.connector

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.types.Row
import org.mockserver.client.MockServerClient
import org.mockserver.model.HttpRequest.request
import org.mockserver.model.HttpResponse.response
import org.mockserver.verify.VerificationTimes
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.testcontainers.containers.MockServerContainer
import org.testcontainers.utility.DockerImageName

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._
import scala.concurrent.duration._
import org.mockserver.matchers.Times
import org.apache.flink.configuration.Configuration
import org.apache.flink.configuration.RestartStrategyOptions
import org.scalatest.BeforeAndAfterAll

class HttpLookupConnectorIntegrationTest
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterEach 
    with BeforeAndAfterAll {

  // Define the MockServer container
  private val container = new MockServerContainer(
    DockerImageName.parse("mockserver/mockserver:mockserver-5.15.0")
  )

  // Setup MockServer client
  var mockServerClient: MockServerClient = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    container.start()
    mockServerClient = new MockServerClient(
      container.getHost,
      container.getServerPort
    ) 
  }

  override def afterAll(): Unit = {
    container.stop()
    super.afterAll()
  }

  override def beforeEach(): Unit = {
    mockServerClient.reset()
  }

  // Mock user data
  val mockUsersJson = """[
    { "id": 1, "name": "Mock Leanne Graham", "username": "Mock Bret", "email": "Sincere@april.biz" },
    { "id": 2, "name": "Mock Ervin Howell", "username": "Mock Antonette", "email": "Shanna@melissa.tv" },
    { "id": 3, "name": "Mock Clementine Bauch", "username": "Mock Samantha", "email": "Nathan@yesenia.net" },
    { "id": 4, "name": "Mock Patricia Lebsack", "username": "Mock Karianne", "email": "Julianne.OConner@kory.org" },
    { "id": 5, "name": "Mock Chelsey Dietrich", "username": "Mock Kamren", "email": "Lucio_Hettinger@annie.ca" },
    { "id": 6, "name": "Mock Mrs. Dennis Schulist", "username": "Mock Leopoldo_Corkery", "email": "Karley_Dach@jasper.info" },
    { "id": 7, "name": "Mock Kurtis Weissnat", "username": "Mock Elwyn.Skiles", "email": "Telly.Hoeger@billy.biz" },
    { "id": 8, "name": "Mock Nicholas Runolfsdottir V", "username": "Mock Maxime_Nienow", "email": "Sherwood@rosamond.me" },
    { "id": 9, "name": "Mock Glenna Reichert", "username": "Mock Delphine", "email": "Chaim_McDermott@dana.io" },
    { "id": 10, "name": "Mock Clementina DuBuque", "username": "Mock Moriah.Stanton", "email": "Rey.Padberg@karina.biz" }
  ]"""

  // Updated mock user data for cache refresh test
  val updatedMockUsersJson = """[
    { "id": 1, "name": "Updated Leanne Graham", "username": "Updated Bret", "email": "Updated@april.biz" },
    { "id": 2, "name": "Updated Ervin Howell", "username": "Updated Antonette", "email": "Updated@melissa.tv" },
    { "id": 3, "name": "Updated Clementine Bauch", "username": "Updated Samantha", "email": "Updated@yesenia.net" },
    { "id": 4, "name": "Updated Patricia Lebsack", "username": "Updated Karianne", "email": "Updated@kory.org" },
    { "id": 5, "name": "Updated Chelsey Dietrich", "username": "Updated Kamren", "email": "Updated@annie.ca" },
    { "id": 6, "name": "Updated Mrs. Dennis Schulist", "username": "Updated Leopoldo_Corkery", "email": "Updated@jasper.info" },
    { "id": 7, "name": "Updated Kurtis Weissnat", "username": "Updated Elwyn.Skiles", "email": "Updated@billy.biz" },
    { "id": 8, "name": "Updated Nicholas Runolfsdottir V", "username": "Updated Maxime_Nienow", "email": "Updated@rosamond.me" },
    { "id": 9, "name": "Updated Glenna Reichert", "username": "Updated Delphine", "email": "Updated@dana.io" },
    { "id": 10, "name": "Updated Clementina DuBuque", "username": "Updated Moriah.Stanton", "email": "Updated@karina.biz" }
  ]"""

  "HttpLookupConnector" should "perform lookup joins with mock HTTP data" in {
    // Setup expectations for this specific test
    mockServerClient
      .when(
        request()
          .withMethod("GET")
          .withPath("/users")
      )
      .respond(
        response()
          .withStatusCode(200)
          .withHeader("Content-Type", "application/json")
          .withBody(mockUsersJson)
      )

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val config = new Configuration()
    config.set(RestartStrategyOptions.RESTART_STRATEGY, "none")
    env.configure(config)
    val settings = EnvironmentSettings.newInstance().inStreamingMode().build()
    val tableEnv = StreamTableEnvironment.create(env, settings)

    // Decrease parallelism for local testing
    env.setParallelism(1)

    // Create the source orders table
    tableEnv.executeSql(
      """
        |CREATE TABLE orders (
        |  order_id STRING,
        |  user_id INT,
        |  amount DOUBLE,
        |  proc_time AS PROCTIME()
        |) WITH (
        |  'connector' = 'datagen',
        |  'rows-per-second' = '1',
        |  'fields.order_id.length' = '5',
        |  'fields.user_id.kind' = 'sequence',
        |  'fields.user_id.start' = '1',
        |  'fields.user_id.end' = '3',
        |  'fields.amount.min' = '10.0',
        |  'fields.amount.max' = '100.0'
        |)
        |""".stripMargin)

    // Create the HTTP lookup table pointing to the mock server
    tableEnv.executeSql(s"""
        |CREATE TABLE user_profiles (
        |  id INT,
        |  name STRING,
        |  username STRING,
        |  email STRING
        |) WITH (
        |  'connector' = 'http-lookup-full-cache',
        |  'url' = 'http://${container.getHost}:${container.getServerPort}/users',
        |  'xpath' = '',
        |  'cache.refresh-interval' = 'PT1M'
        |)
        |""".stripMargin)

    // Perform the lookup join
    val resultTable = tableEnv.sqlQuery("""
      |SELECT
      |  o.order_id,
      |  o.user_id,
      |  o.amount,
      |  u.name,
      |  u.username,
      |  u.email
      |FROM orders AS o
      |JOIN user_profiles FOR SYSTEM_TIME AS OF o.proc_time AS u
      |ON o.user_id = u.id
      |""".stripMargin)

    // Collect results
    val results = new ArrayBuffer[Row]()
    val resultIterator = resultTable.executeAndCollect() 
    var count = 0
    while (resultIterator.hasNext && count < 3) {
      results += resultIterator.next()
      count += 1
    }
    resultIterator.close()

    // Define expected results based on mock data
    val expectedResults = Seq(
      Row.of("generated_order_id_1", Integer.valueOf(1), Double.box(55.0), "Mock Leanne Graham", "Mock Bret", "Sincere@april.biz"),
      Row.of("generated_order_id_2", Integer.valueOf(2), Double.box(55.0), "Mock Ervin Howell", "Mock Antonette", "Shanna@melissa.tv"),
      Row.of("generated_order_id_3", Integer.valueOf(3), Double.box(55.0), "Mock Clementine Bauch", "Mock Samantha", "Nathan@yesenia.net")
    )

    // Validate results (ignoring order_id and amount as they are random)
    results should have size 3
    results.sortBy(_.getFieldAs[Integer]("user_id")).zip(expectedResults).foreach {
      case (actual, expected) =>
        actual.getFieldAs[Integer]("user_id") shouldBe expected.getFieldAs[Integer](1)
        actual.getFieldAs[String]("name") shouldBe expected.getFieldAs[String](3)
        actual.getFieldAs[String]("username") shouldBe expected.getFieldAs[String](4)
        actual.getFieldAs[String]("email") shouldBe expected.getFieldAs[String](5)
    }
    
    // Verify that the mock server was called exactly once for this test
    mockServerClient.verify(
      request()
        .withMethod("GET")
        .withPath("/users"),
      VerificationTimes.exactly(1)
    )
  }

  it should "contain 10 records in user profiles table using mock data" in {
    // Setup expectations for this specific test
    mockServerClient
      .when(
        request()
          .withMethod("GET")
          .withPath("/users")
      )
      .respond(
        response()
          .withStatusCode(200)
          .withHeader("Content-Type", "application/json")
          .withBody(mockUsersJson)
      )

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val config = new Configuration()
    config.set(RestartStrategyOptions.RESTART_STRATEGY, "none")
    env.configure(config)
    val settings = EnvironmentSettings.newInstance().inStreamingMode().build()
    val tableEnv = StreamTableEnvironment.create(env, settings)
    env.setParallelism(1)

    // Create a source table with IDs 1-10
    tableEnv.executeSql("""
        |CREATE TABLE source_ids (
        |  id INT,
        |  proc_time AS PROCTIME()
        |) WITH (
        |  'connector' = 'datagen',
        |  'rows-per-second' = '1',
        |  'fields.id.kind' = 'sequence',
        |  'fields.id.start' = '1',
        |  'fields.id.end' = '10'
        |)
        |""".stripMargin)

    // Create the HTTP lookup table pointing to the mock server
    tableEnv.executeSql(s"""
        |CREATE TABLE user_profiles (
        |  id INT,
        |  name STRING,
        |  username STRING,
        |  email STRING
        |) WITH (
        |  'connector' = 'http-lookup-full-cache',
        |  'url' = 'http://${container.getHost}:${container.getServerPort}/users',
        |  'xpath' = '',
        |  'cache.refresh-interval' = 'PT1M'
        |)
        |""".stripMargin)

    // Query user profiles by joining with source_ids
    val resultTable = tableEnv.sqlQuery("""
      |SELECT u.id, u.name, u.username, u.email
      |FROM source_ids s
      |JOIN user_profiles FOR SYSTEM_TIME AS OF s.proc_time AS u
      |ON s.id = u.id
      |""".stripMargin)

    // Collect results
    val results = new ArrayBuffer[Row]()
    val resultIterator = resultTable.executeAndCollect()
    var count = 0
    while (resultIterator.hasNext && count < 10) {
      results += resultIterator.next()
      count += 1
    }
    resultIterator.close()

    // Verify we got exactly 10 records
    results should have size 10

    // Verify content of the first record (optional, for sanity check)
    val firstRecord = results.find(_.getFieldAs[Integer]("id") == 1).get
    firstRecord.getFieldAs[String]("name") shouldBe "Mock Leanne Graham"
    firstRecord.getFieldAs[String]("username") shouldBe "Mock Bret"
    firstRecord.getFieldAs[String]("email") shouldBe "Sincere@april.biz"
    
    // Verify that the mock server was called exactly once for this test
    mockServerClient.verify(
      request()
        .withMethod("GET")
        .withPath("/users"),
      VerificationTimes.exactly(1)
    )
  }
  
  it should "handle request failures and retries successfully" in {
    // Setup expectations for this test - fail twice then succeed
    var requestCount = 0
    
    // First request - fail with 500
    mockServerClient
      .when(
        request()
          .withMethod("GET")
          .withPath("/users"),
        Times.once()
      )
      .respond(
        response()
          .withStatusCode(500)
          .withHeader("Content-Type", "application/json")
          .withBody("""{"error": "Internal Server Error"}""")
      )
    
    // Second request - fail with 500
    mockServerClient
      .when(
        request()
          .withMethod("GET")
          .withPath("/users"),
        Times.once()
      )
      .respond(
        response()
          .withStatusCode(500)
          .withHeader("Content-Type", "application/json")
          .withBody("""{"error": "Internal Server Error"}""")
      )
    
    // Third request - succeed with 200
    mockServerClient
      .when(
        request()
          .withMethod("GET")
          .withPath("/users"),
        Times.once()
      )
      .respond(
        response()
          .withStatusCode(200)
          .withHeader("Content-Type", "application/json")
          .withBody(mockUsersJson)
      )

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val config = new Configuration()
    config.set(RestartStrategyOptions.RESTART_STRATEGY, "none")
    env.configure(config)
    
    val settings = EnvironmentSettings.newInstance().inStreamingMode().build()
    val tableEnv = StreamTableEnvironment.create(env, settings)
    env.setParallelism(1)

    // Create a source table with IDs 1-3
    tableEnv.executeSql("""
        |CREATE TABLE source_ids (
        |  id INT,
        |  proc_time AS PROCTIME()
        |) WITH (
        |  'connector' = 'datagen',
        |  'rows-per-second' = '1',
        |  'fields.id.kind' = 'sequence',
        |  'fields.id.start' = '1',
        |  'fields.id.end' = '3'
        |)
        |""".stripMargin)

    // Create the HTTP lookup table pointing to the mock server with retry settings
    tableEnv.executeSql(s"""
        |CREATE TABLE user_profiles (
        |  id INT,
        |  name STRING,
        |  username STRING,
        |  email STRING
        |) WITH (
        |  'connector' = 'http-lookup-full-cache',
        |  'url' = 'http://${container.getHost}:${container.getServerPort}/users',
        |  'xpath' = '',
        |  'cache.refresh-interval' = 'PT1M',
        |  'max.retries' = '3',
        |  'retry.delay.ms' = '100'
        |)
        |""".stripMargin)

    // Query user profiles by joining with source_ids
    val resultTable = tableEnv.sqlQuery("""
      |SELECT u.id, u.name, u.username, u.email
      |FROM source_ids s
      |JOIN user_profiles FOR SYSTEM_TIME AS OF s.proc_time AS u
      |ON s.id = u.id
      |""".stripMargin)

    // Collect results
    val results = new ArrayBuffer[Row]()
    val resultIterator = resultTable.executeAndCollect()
    var count = 0
    while (resultIterator.hasNext && count < 3) {
      results += resultIterator.next()
      count += 1
    }
    resultIterator.close()

    // Verify we got exactly 3 records despite the initial failures
    results should have size 3

    // Verify content of the first record
    val firstRecord = results.find(_.getFieldAs[Integer]("id") == 1).get
    firstRecord.getFieldAs[String]("name") shouldBe "Mock Leanne Graham"
    firstRecord.getFieldAs[String]("username") shouldBe "Mock Bret"
    firstRecord.getFieldAs[String]("email") shouldBe "Sincere@april.biz"
    
    // Verify that the mock server was called exactly 3 times (2 failures + 1 success)
    mockServerClient.verify(
      request()
        .withMethod("GET")
        .withPath("/users"),
      VerificationTimes.exactly(3)
    )
  }
  
  it should "refresh cache with updated data" in {
    // Setup initial expectations
    mockServerClient
      .when(
        request()
          .withMethod("GET")
          .withPath("/users")
      )
      .respond(
        response()
          .withStatusCode(200)
          .withHeader("Content-Type", "application/json")
          .withBody(mockUsersJson)
      )

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val config = new Configuration()
    config.set(RestartStrategyOptions.RESTART_STRATEGY, "none")
    env.configure(config)
    val settings = EnvironmentSettings.newInstance().inStreamingMode().build()
    val tableEnv = StreamTableEnvironment.create(env, settings)
    env.setParallelism(1)

    // Create a source table with IDs 1-3
    tableEnv.executeSql("""
        |CREATE TABLE source_ids (
        |  id INT,
        |  proc_time AS PROCTIME()
        |) WITH (
        |  'connector' = 'datagen',
        |  'rows-per-second' = '1',
        |  'fields.id.kind' = 'sequence',
        |  'fields.id.start' = '1',
        |  'fields.id.end' = '3'
        |)
        |""".stripMargin)

    // Create the HTTP lookup table with a short refresh interval for testing
    tableEnv.executeSql(s"""
        |CREATE TABLE user_profiles (
        |  id INT,
        |  name STRING,
        |  username STRING,
        |  email STRING
        |) WITH (
        |  'connector' = 'http-lookup-full-cache',
        |  'url' = 'http://${container.getHost}:${container.getServerPort}/users',
        |  'xpath' = '',
        |  'cache.refresh-interval' = 'PT5S'
        |)
        |""".stripMargin)

    // Query user profiles by joining with source_ids
    val resultTable = tableEnv.sqlQuery("""
      |SELECT u.id, u.name, u.username, u.email
      |FROM source_ids s
      |JOIN user_profiles FOR SYSTEM_TIME AS OF s.proc_time AS u
      |ON s.id = u.id
      |""".stripMargin)

    // Collect initial results
    val initialResults = new ArrayBuffer[Row]()
    val initialIterator = resultTable.executeAndCollect()
    var count = 0
    while (initialIterator.hasNext && count < 3) {
      initialResults += initialIterator.next()
      count += 1
    }
    initialIterator.close()

    // Verify initial results
    initialResults should have size 3
    val initialFirstRecord = initialResults.find(_.getFieldAs[Integer]("id") == 1).get
    initialFirstRecord.getFieldAs[String]("name") shouldBe "Mock Leanne Graham"
    
    // Update the mock server response to return updated data
    mockServerClient
      .when(
        request()
          .withMethod("GET")
          .withPath("/users")
      )
      .respond(
        response()
          .withStatusCode(200)
          .withHeader("Content-Type", "application/json")
          .withBody(updatedMockUsersJson)
      )
    
    // Wait for cache refresh interval to pass
    Thread.sleep(6000)
    
    // Collect results after cache refresh
    val updatedResults = new ArrayBuffer[Row]()
    val updatedIterator = resultTable.executeAndCollect()
    count = 0
    while (updatedIterator.hasNext && count < 3) {
      updatedResults += updatedIterator.next()
      count += 1
    }
    updatedIterator.close()
    
    // Verify updated results
    updatedResults should have size 3
    val updatedFirstRecord = updatedResults.find(_.getFieldAs[Integer]("id") == 1).get
    updatedFirstRecord.getFieldAs[String]("name") shouldBe "Updated Leanne Graham"
    
    // Verify that the mock server was called at least twice (initial + refresh)
    mockServerClient.verify(
      request()
        .withMethod("GET")
        .withPath("/users"),
      VerificationTimes.atLeast(2)
    )
  }
  
  it should "fail the stream when a request fails after a successful one" in {
    // Setup expectations - first request succeeds, second fails
    mockServerClient
      .when(
        request()
          .withMethod("GET")
          .withPath("/users"),
        Times.once()
      )
      .respond(
        response()
          .withStatusCode(200)
          .withHeader("Content-Type", "application/json")
          .withBody(mockUsersJson)
      )
    
    // Second request will fail
    mockServerClient
      .when(
        request()
          .withMethod("GET")
          .withPath("/users"),
        Times.unlimited()
      )
      .respond(
        response()
          .withStatusCode(500)
          .withHeader("Content-Type", "application/json")
          .withBody("""{"error": "Internal Server Error"}""")
      )

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val config = new Configuration()
    config.set(RestartStrategyOptions.RESTART_STRATEGY, "none")
    env.configure(config)
    
    val settings = EnvironmentSettings.newInstance().inStreamingMode().build()
    val tableEnv = StreamTableEnvironment.create(env, settings)
    env.setParallelism(1)

    // Create a source table with IDs 1-3
    tableEnv.executeSql("""
        |CREATE TABLE source_ids (
        |  id INT,
        |  proc_time AS PROCTIME()
        |) WITH (
        |  'connector' = 'datagen',
        |  'rows-per-second' = '1',
        |  'fields.id.kind' = 'sequence',
        |  'fields.id.start' = '1',
        |  'fields.id.end' = '3'
        |)
        |""".stripMargin)

    // Create the HTTP lookup table with a short refresh interval for testing
    tableEnv.executeSql(s"""
        |CREATE TABLE user_profiles (
        |  id INT,
        |  name STRING,
        |  username STRING,
        |  email STRING
        |) WITH (
        |  'connector' = 'http-lookup-full-cache',
        |  'url' = 'http://${container.getHost}:${container.getServerPort}/users',
        |  'xpath' = '',
        |  'cache.refresh-interval' = 'PT1S',
        |  'max.retries' = '1'
        |)
        |""".stripMargin)

    // Query user profiles by joining with source_ids
    val resultTable = tableEnv.sqlQuery("""
      |SELECT u.id, u.name, u.username, u.email
      |FROM source_ids s
      |JOIN user_profiles FOR SYSTEM_TIME AS OF s.proc_time AS u
      |ON s.id = u.id
      |""".stripMargin)

    // Collect initial results - this should succeed
    val initialResults = new ArrayBuffer[Row]()
    val initialIterator = resultTable.executeAndCollect()
    var count = 0
    while (initialIterator.hasNext && count < 3) {
      initialResults += initialIterator.next()
      count += 1
    }
    initialIterator.close()

    // Verify initial results
    initialResults should have size 3
    val initialFirstRecord = initialResults.find(_.getFieldAs[Integer]("id") == 1).get
    initialFirstRecord.getFieldAs[String]("name") shouldBe "Mock Leanne Graham"
    
    // Wait for cache refresh interval to pass
    Thread.sleep(3000)
    
    // Try to collect results after cache refresh - this should fail
    val exception = intercept[Exception] {
      val updatedIterator = resultTable.executeAndCollect()
      var count = 0
      while (updatedIterator.hasNext && count < 3) {
        updatedIterator.next()
        count += 1
      }
      updatedIterator.close()
    }
    
    // Verify that the exception contains information about the HTTP error
    exception.getMessage should include("500")
    
    // Verify that the mock server was called exactly twice (success + failure)
    mockServerClient.verify(
      request()
        .withMethod("GET")
        .withPath("/users"),
      VerificationTimes.exactly(2)
    )
  }
} 