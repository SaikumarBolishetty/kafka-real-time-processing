
# Kafka Real Time Processing Data Pipeline

## Overview
This project implements a real-time streaming data pipeline using Kafka. It consists of a producer that sends user login data to a Kafka topic and a consumer that processes the data.

## Understanding the Pipeline
A data pipeline is a series of steps that move data from one place to another, performing transformations along the way. In this case, you have a simple Kafka pipeline with the following components:
- **Producer**: Produces raw user login data into the Kafka topic `user-login`.
- **Consumer**: Subscribes to the `user-login` topic, processes the data (e.g., filtering, transforming), and can push the processed data into another Kafka topic `processed-user-login`.

The entire flow from producing data, processing it, and potentially storing it elsewhere makes this setup a real-time streaming data pipeline.

## Why It’s Real-Time
- **Continuous Data Flow**: Kafka is a distributed event streaming platform designed to handle real-time data. When the producer is continuously sending data to the `user-login` topic and the consumer is constantly consuming and processing it, the whole system is working in real time.
- **No Manual Start/Stop**: Unlike batch pipelines, where you might schedule or trigger processes manually, real-time pipelines like this don’t have a “start” or “stop” button. They continuously run as long as data is flowing. In real life, you would deploy both the producer and consumer to run continuously, often with monitoring systems in place to ensure everything is functioning correctly.

## How It Works
- **Producer**: Keeps sending data continuously to the Kafka topic.
- **Consumer**: The consumer script, which you've implemented, is subscribed to that Kafka topic and processes the incoming data on the fly.
- **Real-Time**: The moment the producer sends data, the consumer picks it up and processes it without delays (depending on the configurations, the lag is minimal).


## Installation Instructions
1. **Clone the Repository**
   ```bash
   git clone https://github.com/SaikumarBolishetty/real-time-processing.git
   cd cd real-time-processing
   ```

2. **Install Docker and Docker Compose**
   Make sure you have Docker installed on your machine. You can download it from [Docker's official website](https://www.docker.com/get-started).
   Ensure you have Docker Compose installed. If you have Docker Desktop, Docker Compose is included by default. You can verify the installation by running:
   ```bash
   docker compose version
   ```

3. **Build and Start Docker Containers**
   There is no need to install Python dependencies manually since this is handled inside the Docker container. Use Docker Compose to build the Docker images (which includes the Python environment and dependencies) and start the Kafka, Zookeeper, and consumer containers:
   ```bash
   docker compose up -d --build
   ```
4. **Check the Logs**
   Once the containers are running, you can check the logs to verify that everything is working properly:
   ```bash
   docker compose logs python-consumer
   ```

## Running the Application
### Producer
The producer is already running in the Docker container and is generating user login messages that are being sent to the `user-login` Kafka topic. No additional steps are needed to start the producer manually.

### Consumer
The consumer, which is responsible for subscribing to the `user-login` topic, processing the messages (e.g., filtering, transforming), and pushing the processed data to `processed-user-login`, runs automatically inside the Docker container.

You can monitor the logs to ensure that the consumer is processing messages from the user-login topic:
```bash
docker compose logs python-consumer
```

## Viewing Kafka Topics and Logs
1. **To view the existing Kafka topics:**
   ```bash
   docker exec -it real-time-processing-kafka-1 kafka-topics --list --bootstrap-server kafka:9092
   ```

2. **To view the logs of a specific topic (e.g., `user-login`):**

   **user-login topic**
   ```bash
   docker exec -it real-time-processing-kafka-1 kafka-console-consumer --topic user-login --from-beginning --bootstrap-server kafka:9092
   ```
   Sample Input: {"user_id": "63cd3c70-6c44-4971-9fc9-6c45612f31c9", "app_version": "2.3.0", "ip": "91.42.227.164", "locale": "NV", "device_id": "93f1cd25-fe64-4f2c-b574-f6c0345c3c8a", "timestamp": 1729741922, "device_type": "iOS"}

   **processed-user-login topic**
   ```bash
   docker exec -it real-time-processing-kafka-1 kafka-console-consumer --topic user-login --from-beginning --bootstrap-server kafka:9092
   ```
   Sample Output: {"user_id": "febe8e82-8654-4af1-aa7d-4adc0818b2fa", "app_version": "2.3.0", "ip": "82.174.172.125", "locale": "NC", "device_id": "05e9fd85-606a-4744-a8c9-45eef16f19c9", "timestamp": 1729742327, "device_type": "android", "processed_timestamp": 1729742327}

## Examples
1. **Producing a Message:**
   The producer generates messages automatically, so you don't need to manually produce messages.

2. **Consuming a Message:**
   Once you run the consumer, you will see the processed user login messages in your terminal.

## Additional Notes
- Ensure that Docker is running before executing the commands.
- If you encounter any issues, check the logs of the Docker containers for more information:
   ```bash
   docker logs real-time-streaming-kafka-1
   docker logs real-time-streaming-my-python-producer-1
   ``
   
### Deploying in Production
To deploy this Kafka-based application in a production environment, the following steps would be taken:
- **Use a managed Kafka service** (e.g., AWS MSK or Confluent Cloud) to avoid managing Kafka brokers manually and ensure high availability.
- Containerize the application using Docker and orchestrate with **Kubernetes** for easy scaling and management of both Kafka and the producer/consumer services.
- Set up **CI/CD pipelines** (e.g., Jenkins, GitLab) to automate testing, building, and deployment to ensure a reliable, repeatable deployment process.
- Ensure **secure communications** using **TLS encryption** for Kafka brokers and **access control** via SASL/SSL or OAuth.
- Monitor performance with **Prometheus/Grafana** and set up logging systems using the **ELK stack** or AWS CloudWatch for centralized log management.

### Making the Application Production-Ready
To make the application robust for production, additional components and strategies would be required:
- **Error Handling and Retry Mechanisms**: Implement **dead-letter queues** (DLQs) for failed messages and automatic retry logic for transient failures.
- **Monitoring and Alerting**: Integrate **Prometheus** for real-time monitoring of Kafka metrics (e.g., consumer lag) and alerting via **Grafana** dashboards or cloud services like AWS CloudWatch.
- **Security Enhancements**: Implement **TLS encryption** for broker communications and **SASL/SSL authentication** to ensure secure access.
- **Schema Registry**: Use **Kafka Schema Registry** to validate and enforce data schemas, preventing schema evolution issues between producers and consumers.

### Scaling the Application
As the dataset grows, this application can scale by:
- **Partitioning Topics**: Increase the number of Kafka partitions to distribute the load across multiple brokers and enable parallel processing by consumers.
- **Auto-Scaling Consumers**: Use **Kubernetes auto-scaling** policies to dynamically increase the number of consumer instances based on message throughput.
- **Optimizing Resource Allocation**: Continuously monitor and optimize the resources (CPU, memory) allocated to Kafka brokers and consumers, scaling them based on the processing needs.
- **Load Balancing**: Implement **load balancers** to evenly distribute traffic and manage consumer loads.