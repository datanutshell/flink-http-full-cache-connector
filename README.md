# Flink HTTP Lookup Connector

[![CI](https://github.com/dataengnutshell/flink-http-full-cache-connector/actions/workflows/ci.yml/badge.svg)](https://github.com/dataengnutshell/flink-http-full-cache-connector/actions/workflows/ci.yml)
[![Release](https://github.com/dataengnutshell/flink-http-full-cache-connector/actions/workflows/release.yml/badge.svg)](https://github.com/dataengnutshell/flink-http-full-cache-connector/actions/workflows/release.yml)
[![Maven Central](https://img.shields.io/maven-central/v/com.datanutshell.flink/flink-http-lookup-connector.svg?label=Maven%20Central)](https://central.sonatype.com/artifact/com.datanutshell.flink/flink-http-lookup-connector)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

Apache Flink connector for HTTP-based lookups with comprehensive caching support, enabling efficient data enrichment in streaming applications.

## Features

- 🚀 **High Performance**: Full cache loading with configurable refresh intervals
- 🔄 **Automatic Refresh**: Configurable cache refresh strategies
- 🛡️ **Fault Tolerant**: Built-in retry mechanisms and error handling
- 🎯 **Easy Integration**: Simple SQL DDL configuration
- ⚡ **Low Latency**: In-memory caching for sub-millisecond lookups

## Quick Start

### Maven Dependency

```xml
<dependency>
    <groupId>com.datanutshell.flink</groupId>
    <artifactId>flink-http-lookup-connector</artifactId>
    <version>1.0.0</version>
</dependency>
```

### Gradle Dependency

```gradle
implementation 'com.datanutshell.flink:flink-http-lookup-connector:1.0.0'
```

### Basic Usage

Create a lookup table using SQL DDL:

```sql
CREATE TABLE user_lookup (
  id INT,
  name STRING,
  email STRING,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'http-lookup-full-cache',
  'url' = 'https://api.example.com/users',
  'cache.refresh-interval' = 'PT10M',
  'method' = 'GET'
);
```

Use it in a lookup join:

```sql
SELECT 
  e.user_id,
  e.event_type,
  u.name,
  u.email
FROM user_events e
LEFT JOIN user_lookup FOR SYSTEM_TIME AS OF e.proc_time AS u
  ON e.user_id = u.id;
```

## Configuration Options

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `connector` | String | Yes | - | Must be `http-lookup-full-cache` |
| `url` | String | Yes | - | HTTP endpoint URL |
| `method` | String | No | `GET` | HTTP method (GET, POST, etc.) |
| `cache.refresh-interval` | Duration | No | `PT1H` | Cache refresh interval (ISO-8601 duration) |
| `xpath` | String | No | `` | XPath expression for data extraction |
| `connect.timeout.seconds` | Integer | No | `10` | Connection timeout in seconds |
| `read.timeout.seconds` | Integer | No | `30` | Read timeout in seconds |
| `max.retries` | Integer | No | `3` | Maximum number of retries |
| `retry.delay.ms` | Long | No | `1000` | Delay between retries in milliseconds |

## Examples

### Basic User Lookup

```sql
CREATE TABLE user_lookup (
  id INT,
  name STRING,
  username STRING,
  email STRING,
  phone STRING,
  website STRING,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'http-lookup-full-cache',
  'url' = 'https://jsonplaceholder.typicode.com/users',
  'cache.refresh-interval' = 'PT10M',
  'method' = 'GET',
  'connect.timeout.seconds' = '10',
  'read.timeout.seconds' = '30',
  'max.retries' = '3',
  'retry.delay.ms' = '1000'
);
```

### Real-time Event Enrichment

```sql
-- Source table with events
CREATE TABLE user_events (
  user_id INT,
  event_type STRING,
  event_time TIMESTAMP(3),
  proc_time AS PROCTIME()
) WITH (
  'connector' = 'kafka',
  'topic' = 'user-events',
  'properties.bootstrap.servers' = 'localhost:9092',
  'format' = 'json'
);

-- Enriched output
INSERT INTO enriched_events
SELECT 
  e.user_id,
  e.event_type,
  e.event_time,
  u.name,
  u.email
FROM user_events e
LEFT JOIN user_lookup FOR SYSTEM_TIME AS OF e.proc_time AS u
  ON e.user_id = u.id;
```

## Architecture

The connector implements a full-cache strategy where:

1. **Initial Load**: All data is loaded from the HTTP endpoint at startup
2. **Periodic Refresh**: Cache is refreshed at configurable intervals
3. **In-Memory Storage**: Data is stored in memory for fast lookups
4. **Fault Tolerance**: Automatic retries and error handling

```
┌─────────────────┐    HTTP Request    ┌─────────────────┐
│                 │ ──────────────────▶│                 │
│  Flink Job      │                    │  HTTP Endpoint  │
│                 │ ◀────────────────── │                 │
└─────────────────┘    JSON Response   └─────────────────┘
         │                                       ▲
         ▼                                       │
┌─────────────────┐                             │
│   In-Memory     │                             │
│     Cache       │                             │
│                 │                             │
└─────────────────┘                             │
         │                              Periodic Refresh
         ▼                                       │
┌─────────────────┐                             │
│   Lookup Join   │ ────────────────────────────┘
│    Operation    │
└─────────────────┘
```

## Development

### Prerequisites

- Java 11 or higher
- Apache Flink 1.17+
- Gradle 8.0+

### Building from Source

```bash
# Clone the repository
git clone https://github.com/dataengnutshell/flink-http-full-cache-connector.git
cd flink-http-full-cache-connector

# Build the project
./gradlew build

# Run tests
./gradlew test

# Run integration tests
./gradlew integrationTest
```

### Running the Example

```bash
cd example
./gradlew run
```

This will start a Flink job that demonstrates the HTTP lookup connector using the JSONPlaceholder API.

## Monitoring and Metrics

The connector provides comprehensive metrics for monitoring:

- **Cache Hit Rate**: Percentage of successful cache lookups
- **Cache Refresh Duration**: Time taken to refresh the cache
- **HTTP Request Metrics**: Success/failure rates, response times
- **Error Rates**: Retry attempts and failure counts

Access these metrics through Flink's metrics system and your monitoring infrastructure.

## Performance Considerations

### Cache Sizing

- Monitor memory usage when caching large datasets
- Consider the trade-off between refresh frequency and data freshness
- Use appropriate JVM heap sizing for your cache requirements

### Network Optimization

- Set appropriate timeout values for your network conditions
- Configure retry strategies based on endpoint reliability
- Consider using connection pooling for high-throughput scenarios

### Refresh Strategy

```sql
-- Frequent updates for critical data
'cache.refresh-interval' = 'PT1M'  -- Every minute

-- Balanced approach for most use cases
'cache.refresh-interval' = 'PT10M' -- Every 10 minutes

-- Infrequent updates for static data
'cache.refresh-interval' = 'PT1H'  -- Every hour
```

## Contributing

We welcome contributions! Please see our [Contributing Guide](CONTRIBUTING.md) for details.

### Development Workflow

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Ensure all tests pass
6. Submit a pull request

### Code Style

This project uses Scalafmt for code formatting:

```bash
./gradlew scalafmtAll
```

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## Support

- 📖 **Documentation**: Check the [docs](docs/) directory
- 🐛 **Issues**: Report bugs on [GitHub Issues](https://github.com/dataengnutshell/flink-http-full-cache-connector/issues)
- 💬 **Discussions**: Join the conversation in [GitHub Discussions](https://github.com/dataengnutshell/flink-http-full-cache-connector/discussions)
---

**Data Nutshell**