# 🚦 NYC Traffic Intelligence Pipeline

An end-to-end real-time data engineering project that ingests, validates, streams, processes, and visualizes live traffic incident data from New York City.

> Inspired by real-world data engineering practices used in broadcasting and public safety systems (e.g., SNRT internship architecture).

---

## 📊 Live Dashboard Features
- Real-time streaming of NYC traffic incidents
- Kafka-powered ingestion from SOAP + REST APIs
- Stream processing with Apache Spark
- Aggregated reporting in PostgreSQL & Cassandra
- Dashboards in both **React** (live) and **Power BI** (analytical)
- SOAP Web Service (.NET Core) simulating legacy systems

---

## 🔧 Tech Stack

| Layer | Tools |
|-------|-------|
| Ingestion | Apache Airflow, Node.js, .NET Core SOAP |
| Streaming | Kafka, Zookeeper, Confluent Schema Registry |
| Processing | Apache Spark Structured Streaming |
| Storage | PostgreSQL (for Power BI), Apache Cassandra |
| Visualization | React.js (real-time), Power BI (analytics) |
| Deployment | Docker Compose, GitHub Actions (CI) |

---

