# Exercise

The following exercise extends the Real-Time Analytics (RTA) in a Day workshop on Microsoft Fabric by adding a  module on Spark Structured Streaming. This exercise is designed to seamlessly integrate with the existing workshop flow while providing participants with practical experience in implementing real-time data processing pipelines using Fabric's Data Engineering Apahce Spark-based  capabilities.

## Introduction to Spark Structured Streaming in Fabric

Spark Structured Streaming represents a paradigm for processing real-time data within Microsoft Fabric. It treats a live data stream as a continuously appended table, enabling developers to use familiar batch-like queries while Spark handles the incremental processing behind the scenes. This approach significantly simplifies building real-time data pipelines while maintaining scalability and fault tolerance.

Microsoft Fabric offers two primary options for real-time data processing: the Real-Time Analytics service and Spark-based streaming (Data Engineering Pillar). This exercise focuses on the latter, exploring how Fabric's implementation of Spark Structured Streaming enables data engineering workflows inside Fabric notebooks with minimal complexity.

### Key Concepts and Architecture

Spark Structured Streaming operates on a micro-batch processing model by default, treating streaming data as small batches to achieve low latencies (as low as 100ms) with exactly-once fault-tolerance guarantees[5]. This model aligns perfectly with the medallion architecture commonly used in Microsoft Fabric data engineering workflows, where data flows through bronze (raw), silver (validated/transformed), and gold (business-ready) layers.

## 1. Environment Setup

### 1.1 New Notebook

Create a New Notebook named e.g. `Spark Structured Streaming` and install required libraries:

```shell
! python --version

! pip install azure-eventhub==5.11.5 faker==24.2.0 pyodbc==5.1.0 --upgrade --force --quiet
```

### 1.2 Event Hub Configuration

Use the same name as previously (exercise 7). # TODO add a link here!  

```python
from azure.eventhub import EventHubProducerClient

# Configure Event Hub connection
eventHubNameevents = "your-event-hub-name"
eventHubConnString = "Endpoint=sb://..."  

producer_events = EventHubProducerClient.from_connection_string(
    conn_str=eventHubConnString, 
    eventhub_name=eventHubNameevents
)
```

## 2. Stream Ingestion Configuration

### 2.1 Event Hubs Connection
```python
ehConf = {
    "eventhubs.connectionString": eventHubConnString,
    "eventhubs.consumerGroup": "$Default",
    "maxEventsPerTrigger": 1000,  # Throughput control - limits how many events Spark will read per trigger cycle, helping control throughput and avoid overwhelming your application.
    "startingPosition": json.dumps({"offset": "-1"}) # Defines where in the stream Spark starts reading. Here, "-1" means start from the earliest available event.
}

raw_stream = spark.readStream \
    .format("eventhubs") \
    .options(**ehConf) \
    .load()
```

## 3. Stream Processing Pipeline

### 3.1 Schema Definition & Parsing
```python
# Define schema for nested JSON structure
from pyspark.sql.types import *
from pyspark.sql.functions import from_json, col

event_schema = StructType([
    StructField("eventType", StringType()),
    StructField("eventID", StringType()),
    StructField("productId", StringType()),
    StructField("userAgent", StructType([
        StructField("platform", StringType()),
        StructField("browser", StringType())
    ])),
    StructField("extraPayload", ArrayType(
        StructType([
            StructField("relatedProductId", StringType()),
            StructField("relatedProductCategory", StringType())
        ])
    ))
])

# Parse JSON payload
parsed_stream = raw_stream.select(
    from_json(col("body").cast("string"), event_schema).alias("data"),
    col("enqueuedTime").alias("processing_time")
).select("data.*", "processing_time")
```

### 3.2 Real-Time Aggregations
```python
from pyspark.sql.functions import window, count, countDistinct

# Windowed aggregations
windowed_agg = parsed_stream \
    .withWatermark("processing_time", "10 minutes") \
    .groupBy(
        window("processing_time", "5 minutes"),
        "eventType",
        "userAgent.platform"
    ).agg(
        count("*").alias("event_count"),
        countDistinct("productId").alias("unique_products")
    )
```

Read about [Window Operations on Event Time](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#window-operations-on-event-time).

## 4. Delta Lake Integration

Create a new lakehouse or use existing!

https://learn.microsoft.com/en-us/fabric/data-engineering/lakehouse-streaming-data


### 4.1 Bronze Layer Ingestion
```python
bronze_write = parsed_stream.writeStream \
    .format("delta") \
    .option("checkpointLocation", "Files/checkpoints/bronze") \
    .outputMode("append") \
    .trigger(processingTime="1 minute") \
    .toTable("bronze_events")
```
If you seek checkpoint explanation, check the bottom part & theory part.


### 4.2 Silver Layer Processing
```python
# Create Silver table view
silver_stream = spark.readStream \
    .table("bronze_events") \
    .withColumn("clickpath", explode("extraPayload"))

# Write to Silver Delta table
silver_write = silver_stream.writeStream \
    .format("delta") \
    .option("checkpointLocation", "Files/checkpoints/silver") \
    .outputMode("append") \
    .trigger(processingTime="5 minutes") \
    .toTable("silver_events")
```

Similar you can continue for Gold Layer. 

## 5. Production Deployment in Fabric

### 5.1 Spark Job Definition (SJD)

    Run streadming jobs continously. Discuss with tutors the options how and via which service you can run it. Use environment and install required libraries on dedicated environment. 
