# Smart City Real-Time Data Handling 🌆

A scalable, real-time data processing pipeline for smart city infrastructure monitoring and analysis.

## Overview 🚀

This project implements a robust, end-to-end data pipeline that processes real-time smart city data, including vehicle movements, GPS tracking, traffic camera feeds, weather information, and emergency incidents. The system handles data ingestion, processing, storage, and analytics using modern cloud-native technologies.

## Architecture 🏗️

![Screenshot (1296)](https://github.com/user-attachments/assets/e06fc286-a75d-423b-af63-5f59c423afd8)



The system employs a microservices architecture with the following components:

- **Data Ingestion:** Apache Kafka for reliable, scalable data streaming
- **Stream Processing:** Apache Spark for real-time data processing and transformation
- **Containerization:** Docker used for containerizing Kafka, Zookeerper and Spark processes for seamless interaction between components
- **Storage:** Amazon S3 for scalable data lake storage
- **Data Catalog:** AWS Glue for automated schema discovery and metadata management
- **Analytics:** AWS Athena for ad-hoc queries and AWS Redshift for complex analytics

## Key Features ✨

- Real-time data ingestion and processing
- Fault-tolerant architecture with data checkpointing
- Scalable stream processing using Spark Structured Streaming
- Automated schema management and data cataloging
- Support for both real-time analytics and historical data analysis

## Technologies Used 💻

- Apache Kafka & Zookeeper
- Apache Spark
- Docker
- AWS Services (S3, Glue, Athena, Redshift)
- Python

## Getting Started 🚦

```bash
# Clone the repository
git clone https://github.com/username/smart-city-data-handling

# Set up environment variables
export KAFKA_BOOTSTRAP_SERVERS='localhost:9092'
export AWS_ACCESS_KEY='your-access-key'
export AWS_SECRET_KEY='your-secret-key'

# Start the services using Docker Compose
docker-compose up -d

# Run the data simulator
python main.py

# Run the Spark processing pipeline
python spark-city.py
```

## Data Simulation 📊

The project includes a data simulator that generates realistic smart city data:

- Vehicle movement patterns between cities
- GPS tracking information
- Traffic camera feeds
- Weather conditions
- Emergency incidents

## Monitoring and Analytics 📈

Access the following interfaces for monitoring and analysis:

- Spark UI: localhost:9090
- Kafka Manager: localhost:9000
- AWS Athena for SQL queries
- Redshift for complex analytics

## Contributing 🤝

Contributions are welcome! Please read our contributing guidelines and submit pull requests for any enhancements.


## Contact 📧

For questions or collaboration opportunities, please reach out to Omkar Yeole at omkaryeole@gmail.com.
