
# Kafka to Cassandra Pipeline with Airflow and Spark Streaming

## Overview

This project implements a real-time data pipeline using Apache Kafka, Apache Spark Streaming, Apache Cassandra, and Apache Airflow. The pipeline is designed to handle data ingestion, processing, and storage from Kafka to Cassandra.

### Components
- **Apache Kafka**: Serves as the message broker for the data pipeline.
- **Apache Spark Streaming**: Handles real-time data processing.
- **Apache Cassandra**: Stores the processed data.
- **Apache Airflow**: Orchestrates the data pipeline, including running the Kafka producer and managing data flow.

## Architecture

1. **Airflow**
   - **PythonOperator**: Runs a Python script that acts as a Kafka producer. This script generates and sends data to a Kafka topic.

2. **Kafka**
   - **Producer**: Created and managed by the Python script executed by Airflow.
   - **Consumer**: Consumed by Spark Streaming. The data is read from Kafka topics.

3. **Spark Streaming**
   - **Stream Processing**: Reads data from Kafka topics and processes it in real-time.
   - **foreachBatch**: Writes the processed data to Cassandra.

4. **Cassandra**
   - **Storage**: Stores the processed data received from Spark Streaming.

### Data Flow
1. **Airflow PythonOperator**:
   - Executes a script to produce data and send it to Kafka topics.
   
2. **Kafka**:
   - The data is published to Kafka topics and is available for consumption.

3. **Spark Streaming**:
   - Consumes data from Kafka topics.
   - Processes the data and writes it to Cassandra using the `foreachBatch` method.

4. **Cassandra**:
   - Receives and stores the processed data.
