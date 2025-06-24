package com.datanutshell.flink.connector

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.types.Row
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks

import java.time.Duration
import scala.collection.mutable.ArrayBuffer

class HttpLookupConnectorTest extends AnyFlatSpec with Matchers with TableDrivenPropertyChecks {

  "HttpLookupConnector" should "perform lookup joins with HTTP data" in {
    // Set up the streaming environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val settings = EnvironmentSettings.newInstance().inStreamingMode().build()
    val tableEnv = StreamTableEnvironment.create(env, settings)

    // Create a dummy temporal table with some data
    tableEnv.executeSql("""
        |CREATE TABLE orders (
        |  order_id STRING,
        |  user_id INT,
        |  amount DOUBLE,
        |  proc_time AS PROCTIME()
        |) WITH (
        |  'connector' = 'datagen',
        |  'rows-per-second' = '1',
        |  'fields.order_id.length' = '5',
        |  'fields.user_id.min' = '1',
        |  'fields.user_id.max' = '10',
        |  'fields.amount.min' = '10.0',
        |  'fields.amount.max' = '1000.0'
        |)
        |""".stripMargin)

    // Create the HTTP lookup table
    tableEnv.executeSql("""
        |CREATE TABLE user_profiles (
        |  id INT,
        |  name STRING,
        |  username STRING,
        |  email STRING
        |) WITH (
        |  'connector' = 'http-lookup-full-cache',
        |  'url' = 'https://jsonplaceholder.typicode.com/users',
        |  'xpath' = '',
        |  'cache.refresh-interval' = 'PT1M'
        |)
        |""".stripMargin)

    // Perform a lookup join between orders and user profiles
    val result = tableEnv.sqlQuery("""
        |SELECT 
        |  o.order_id,
        |  o.user_id,
        |  o.amount,
        |  o.proc_time,
        |  u.name,
        |  u.username,
        |  u.email
        |FROM orders o
        |LEFT JOIN user_profiles FOR SYSTEM_TIME AS OF o.proc_time AS u
        |ON o.user_id = u.id
        |""".stripMargin)

    // Execute the query and collect results
    val resultTable = result.execute()
    val iterator = resultTable.collect()

    // Convert iterator to a list, but limit to 10 elements for testing
    val results = new ArrayBuffer[Row]()
    var count = 0
    while (iterator.hasNext && count < 10) {
      results += iterator.next()
      count += 1
    }
    iterator.close()

    // Verify we got some results
    results should not be empty
    results.size should be <= 10

    // Print the results for inspection
    println(s"Collected ${results.size} results:")
    results.foreach(println)
  }

  it should "contain 10 records in user profiles table" in {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val settings = EnvironmentSettings.newInstance().inStreamingMode().build()
    val tableEnv = StreamTableEnvironment.create(env, settings)

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

    // Create the HTTP lookup table
    tableEnv.executeSql("""
        |CREATE TABLE user_profiles (
        |  id INT,
        |  name STRING,
        |  username STRING,
        |  email STRING
        |) WITH (
        |  'connector' = 'http-lookup-full-cache',
        |  'url' = 'https://jsonplaceholder.typicode.com/users',
        |  'xpath' = '',
        |  'cache.refresh-interval' = 'PT1M'
        |)
        |""".stripMargin)

    // Query user profiles by joining with source_ids
    val result = tableEnv.sqlQuery("""
        |SELECT u.id, u.name, u.username, u.email
        |FROM source_ids s
        |JOIN user_profiles FOR SYSTEM_TIME AS OF s.proc_time AS u
        |ON s.id = u.id
        |""".stripMargin)

    // Execute and collect results
    val resultTable = result.execute()
    val iterator = resultTable.collect()

    val results = new ArrayBuffer[Row]()
    var count = 0
    while (iterator.hasNext && count < 10) {
      results += iterator.next()
      count += 1
    }
    iterator.close()

    // Verify we got exactly 10 records
    results should have size 10

    // Verify each record has the expected fields
    for (row <- results) {
      row.getField(0) shouldNot be(null) // id
      row.getField(1) shouldNot be(null) // name
      row.getField(2) shouldNot be(null) // username
      row.getField(3) shouldNot be(null) // email
    }
  }
}
