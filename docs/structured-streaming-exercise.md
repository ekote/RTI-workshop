# Exercise

The following exercise extends the Real-Time Analytics (RTA) in a Day workshop on Microsoft Fabric by adding a  module on Spark Structured Streaming. This exercise is designed to seamlessly integrate with the existing workshop flow while providing participants with practical experience in implementing real-time data processing pipelines using Fabric's Data Engineering Apahce Spark-based  capabilities.

## Introduction to Spark Structured Streaming in Fabric

Spark Structured Streaming represents a paradigm for processing real-time data within Microsoft Fabric. It treats a live data stream as a continuously appended table, enabling developers to use familiar batch-like queries while Spark handles the incremental processing behind the scenes. This approach significantly simplifies building real-time data pipelines while maintaining scalability and fault tolerance.

Microsoft Fabric offers two primary options for real-time data processing: the Real-Time Analytics service and Spark-based streaming (Data Engineering Pillar). This exercise focuses on the latter, exploring how Fabric's implementation of Spark Structured Streaming enables data engineering workflows inside Fabric notebooks with minimal complexity.

### Key Concepts and Architecture

Spark Structured Streaming operates on a micro-batch processing model by default, treating streaming data as small batches to achieve low latencies (as low as 100ms) with exactly-once fault-tolerance guarantees[5]. This model aligns perfectly with the medallion architecture commonly used in Microsoft Fabric data engineering workflows, where data flows through bronze (raw), silver (validated/transformed), and gold (business-ready) layers.

## 1. Environment Setup

### 1.1 Event Hub Configuration

Use the same name as previously (exercise 7).
```python
# Configure Event Hub connection
eventHubNameevents = "your-event-hub-name"
eventHubConnString = "Endpoint=sb://..."  # From Azure Portal

producer_events = EventHubProducerClient.from_connection_string(
    conn_str=eventHubConnString, 
    eventhub_name=eventHubNameevents
)
```

### 1.2 Spark Session Initialization
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("EventHubStreamProcessing") \
    .config("spark.jars.packages", "com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.22") \
    .getOrCreate()
```

## 2. Stream Ingestion Configuration

### 2.1 Event Hubs Connection
```python
ehConf = {
    "eventhubs.connectionString": eventHubConnString,
    "eventhubs.consumerGroup": "$Default",
    "maxEventsPerTrigger": 1000,  # Throughput control [2][4] - limits how many events Spark will read per trigger cycle, helping control throughput and avoid overwhelming your application.
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
# Windowed aggregations [1][8]
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

### 4.1 Bronze Layer Ingestion
```python
bronze_write = parsed_stream.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/delta/checkpoints/bronze") \
    .outputMode("append") \
    .trigger(processingTime="1 minute") \
    .table("bronze_events")
```
If you seek checkpoint explanation, check the bottom part. 

### 4.2 Silver Layer Processing
```python
# Create Silver table view
silver_stream = spark.readStream \
    .table("bronze_events") \
    .withColumn("clickpath", explode("extraPayload"))

# Write to Silver Delta table
silver_write = silver_stream.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/delta/checkpoints/silver") \
    .outputMode("append") \
    .trigger(processingTime="5 minutes") \
    .table("silver_events")
```

## 5. Production Deployment in Fabric

### 5.1 Spark Job Definition
```bash
# Submit job to Fabric Spark
spark-submit \
    --packages com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.22 \
    --conf spark.sql.streaming.checkpointLocation=/checkpoints \
    your_streaming_job.py
```

### 5.2 Throughput Management[2][4]
```python
# Configure for TU-based throughput
ehConf = EventHubsConf(connectionString) \
    .setStartingPosition(EventPosition.fromEndOfStream) \
    .setMaxRatePerPartition(1000)  # Align with TU capacity
```

## 6. Monitoring & Optimization

### 6.1 Stream Monitoring
```python
# Query progress monitoring
for stream in spark.streams.active:
    print(f"Stream ID: {stream.id}")
    print(f"Status: {stream.status}")
    print(f"Latest progress: {stream.lastProgress}")
```

### 6.2 Anomaly Detection (Extension)
```python
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeansModel

# Load pre-trained model
model = KMeansModel.load("/models/anomaly_detection")

# Detect anomalous page load times
anomalies = parsed_stream \
    .withColumn("load_time", col("page_loading_seconds")) \
    .select("eventID", "load_time") \
    .transform(lambda df: model.transform(df)) \
    .filter(col("prediction") == 0)  # Anomaly cluster
```

## Key Considerations

### 1. **Checkpoint Management**
   - Use separate checkpoint locations for different streams
   - Regularly clean up old checkpoints[2][4]
   
  Here is the full context.

#### 1. Checkpoint Fundamentals
  Checkpoints in Structured Streaming serve as **persistent logs** that track:
     - **Processing progress** (last consumed offsets)
     - **State information** for aggregations and window operations
     - **Metadata** about the streaming query (source/sink configs)
  
  ```python
  # Example from code:
  bronze_write = parsed_stream.writeStream \
      .option("checkpointLocation", "/delta/checkpoints/bronze") \  # [1][4]
      .table("bronze_events")
  ```

#### 2.  Implementation Details
  2.1 Query-Specific Checkpoints
  Each streaming query **must** have a unique checkpoint location:
  
  ```python
  # Bronze layer
  .option("checkpointLocation", "/delta/checkpoints/bronze") 
  
  # Silver layer 
  .option("checkpointLocation", "/delta/checkpoints/silver") 
  ```
  
  **Why this matters:**
  - Prevents metadata conflicts between queries
    - Enables independent recovery of different pipeline stages
    - Maintains separate state stores for different processing logic
  
  ##### 2.2 Checkpoint Composition
  A checkpoint directory contains:
  ```
  ├── commits       # Transaction log
  ├── offsets       # Source offsets per batch
  ├── state         # Aggregation/window states
  └── metadata      # Query configuration
  ```
  
  #### 3. Recovery Mechanisms
  
  ### 3.1 Failure Recovery Flow
  1. **Driver Restart**: Reads metadata to reconstruct query
     2. **Offset Validation**: Confirms stored offsets with source
     3. **State Restoration**: Rebuilds in-memory state from disk
     4. **Processing Resume**: Continues from last committed offset
  
  ### 3.2 Watermark Coordination
  ```python
  .withWatermark("processing_time", "10 minutes") 
  ```
  - Checkpoints store watermark progress to handle:
    - Late-arriving data management
    - State cleanup thresholds
    - Window expiration tracking
  
  ## 4. Production Considerations
  
  ### 4.1 Storage Requirements
  | Feature | Impact on Checkpoints | Source |
  |---------|-----------------------|--|
  | 10 MB/s throughput | ~1 GB/day checkpoints | https://risingwave.com/blog/the-ultimate-guide-to-setting-checkpoint-location-in-spark-streaming/ |
  | 24h retention | 7-14 day storage budget |https://www.reddit.com/r/databricks/comments/1fkwm00/handling_spark_structured_streaming_checkpoint/ |

  **Best Practice:** Use cloud storage with lifecycle policies for automatic cleanup.
  
  ### 4.2 Version Compatibility
  ```python
  # Code impact when modifying:
  .option("checkpointLocation", "/delta/checkpoints/v2") 
  ```
  **Key constraints:**
  - Schema changes require new checkpoint
    - Source/sink modifications need fresh checkpoints
    - Spark version upgrades often need migration
  
  ## 5. Operational Best Practices
  
  ### 5.1 Monitoring Essentials
  ```python
  # From code example:
  for stream in spark.streams.active:
      print(f"Status: {stream.status}")  
      print(f"Progress: {stream.lastProgress}")
  ```
  **Critical metrics:**
  - `numInputRows` vs `processedRowsPerSecond`
    - `stateOperators` memory usage
    - `sources` lag metrics
  
  ### 5.2 Maintenance Checklist
  1. **Daily validation** of checkpoint accessibility
     2. **Weekly cleanup** of orphaned checkpoints
     3. **Monthly testing** of failure recovery process
  
  ## 6. Code-Specific Analysis
  
  ### 6.1 Anomaly Detection Checkpoints
  ```python
  anomalies.writeStream \
      .option("checkpointLocation", "/delta/checkpoints/anomalies") 
  ```
  **Special considerations:**
  - ML model versions must be checkpoint-compatible
    - Feature vectors require schema versioning
    - Model updates need checkpoint migration
  
  ### 6.2 Delta Lake Integration
  ```python
  .format("delta") \  # Implicit checkpoint optimizations
  ```
  **Automatic enhancements:**
  - Transactional writes with ACID guarantees
    - Compaction during checkpoint commits
    - Schema evolution tracking
  
  ## 7. Failure Modes and Mitigations
  
  | Failure Scenario | Checkpoint Behavior | Recovery Action |
  |-------------------|----------------------|-----------------|
  | Driver crash | Auto-restart from last commit | Monitor spark-submit process |
  | Executor failure | Rebuild state from checkpoint | Ensure adequate cluster size |
  | Storage outage | Query termination | Validate storage redundancy |
  | Schema drift | Query failure with SerializationException | Implement schema registry |
  

2. **Schema Evolution**
   - Use `mergeSchema` option for Delta writes
   ```python
   .option("mergeSchema", "true")
   ```

3. **Late Data Handling**
   ```python
   .withWatermark("processing_time", "1 hour")
   ```

4. **Fault Tolerance**
   - Enable write-ahead logs (WAL)
   ```python
   .config("spark.sql.streaming.checkpointLocation", "/checkpoints")
   ```

This implementation follows the medallion architecture pattern and integrates with Fabric's Spark engine capabilities. The solution provides:
- Real-time ingestion from Event Hubs
- Nested JSON parsing
- Windowed aggregations
- Anomaly detection
- Delta Lake integration
- Production-grade monitoring[1][2][4][8]

# References
[1] https://www.youtube.com/watch?v=wo9vhVBUKXI
[2] https://github.com/Azure/azure-event-hubs-spark/blob/master/docs/structured-streaming-eventhubs-integration.md
[3] https://k21academy.com/microsoft-azure/data-engineer/structured-streaming-with-azure-event-hubs/
[4] https://github.com/Azure/azure-event-hubs-spark/blob/master/docs/spark-streaming-eventhubs-integration.md
[5] https://www.youtube.com/watch?v=pwWIegHgNRw
[6] https://learn.microsoft.com/en-us/azure/event-hubs/event-hubs-kafka-spark-tutorial
[7] https://learn.microsoft.com/pl-pl/azure/event-hubs/event-hubs-kafka-spark-tutorial
[8] https://addendanalytics.com/blog/structured-streaming-in-databricks-from-event-hub
