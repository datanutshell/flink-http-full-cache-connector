package com.datanutshell.examples

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.types.Row
import org.slf4j.LoggerFactory

/**
 * Example demonstrating the use of the Flink HTTP Lookup Connector.
 * 
 * This example shows how to:
 * 1. Create a lookup table that fetches data from an HTTP endpoint
 * 2. Join streaming data with the lookup table for enrichment
 * 3. Use caching to optimize performance
 */
object FlinkHttpConnectorExample {
  
  private val logger = LoggerFactory.getLogger(getClass)
  
  def main(args: Array[String]): Unit = {
    // Create Flink execution environment
    val env = StreamExecutionEnvironment.createLocalEnvironment(1)
    env.setParallelism(1) // For this example
    
    // Create Table Environment
    val tableEnv = StreamTableEnvironment.create(env)
    
    // Example 1: Create a lookup table using JSONPlaceholder API
    createUserLookupTable(tableEnv)
    
    // Example 2: Create a streaming source with user IDs
    createUserInputStream(tableEnv)
    
    // Example 3: Join streaming data with lookup table
    performLookupJoin(tableEnv)
    
    // Execute the job
    logger.info("Starting Flink HTTP Connector Example...")
    env.execute("Flink HTTP Connector Example")
  }
  
  /**
   * Creates a lookup table that fetches user data from JSONPlaceholder API
   */
  private def createUserLookupTable(tableEnv: StreamTableEnvironment): Unit = {
    val createTableDDL =
      """
        |CREATE TABLE user_lookup (
        |  id INT,
        |  name STRING,
        |  username STRING,
        |  email STRING,
        |  phone STRING,
        |  website STRING,
        |  PRIMARY KEY (id) NOT ENFORCED
        |) WITH (
        |  'connector' = 'http-lookup-full-cache',
        |  'url' = 'https://jsonplaceholder.typicode.com/users',
        |  'xpath' = '',
        |  'cache.refresh-interval' = 'PT10M',
        |  'method' = 'GET',
        |  'connect.timeout.seconds' = '10',
        |  'read.timeout.seconds' = '30',
        |  'max.retries' = '3',
        |  'retry.delay.ms' = '1000'
        |)
        |""".stripMargin
    
    tableEnv.executeSql(createTableDDL)
    logger.info("Created user_lookup table")
  }
  
  /**
   * Creates a streaming source with user IDs for demonstration
   */
  private def createUserInputStream(tableEnv: StreamTableEnvironment): Unit = {
    val createSourceDDL =
      """
        |CREATE TABLE user_events (
        |  user_id INT,
        |  event_type STRING,
        |  event_time TIMESTAMP(3),
        |  proc_time AS PROCTIME(),
        |  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
        |) WITH (
        |  'connector' = 'datagen',
        |  'rows-per-second' = '1',
        |  'fields.user_id.min' = '1',
        |  'fields.user_id.max' = '10',
        |  'fields.event_type.length' = '10'
        |)
        |""".stripMargin
    
    tableEnv.executeSql(createSourceDDL)
    logger.info("Created user_events source table")
  }
  
  /**
   * Performs a lookup join between streaming events and user data
   */
  private def performLookupJoin(tableEnv: StreamTableEnvironment): Unit = {
    // Create a sink table to output results
    val createSinkDDL =
      """
        |CREATE TABLE enriched_events (
        |  user_id INT,
        |  event_type STRING,
        |  event_time TIMESTAMP(3),
        |  name STRING,
        |  username STRING,
        |  email STRING,
        |  phone STRING,
        |  website STRING
        |) WITH (
        |  'connector' = 'print'
        |)
        |""".stripMargin
    
    tableEnv.executeSql(createSinkDDL)
    
    // Insert results into sink
    val insertQuery =
      """
        |INSERT INTO enriched_events
        |SELECT 
        |  user_id,
        |  event_type,
        |  event_time,
        |  name,
        |  username,
        |  email,
        |  phone,
        |  website
        |FROM (
        |  SELECT 
        |    e.user_id,
        |    e.event_type,
        |    e.event_time,
        |    u.name,
        |    u.username,
        |    u.email,
        |    u.phone,
        |    u.website
        |  FROM user_events e
        |  LEFT JOIN user_lookup FOR SYSTEM_TIME AS OF e.proc_time AS u
        |    ON e.user_id = u.id
        |)
        |""".stripMargin
    
    tableEnv.executeSql(insertQuery)
    logger.info("Started lookup join processing")
  }
} 