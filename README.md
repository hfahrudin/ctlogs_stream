# CT Logs Stream Processor

This repository contains a system designed to fetch, process, and store Certificate Transparency (CT) logs. It ingests CT log entries, extracts relevant certificate information, and stores it in a ClickHouse database for analysis and monitoring.Overcomes stream rate limits by implementing adaptive worker staggering, optimizing request timing for peak ingestion speed.

## How it Works

The system operates in a producer-consumer model using Python's `multiprocessing` for inter-process communication:

1.  **CT Log Fetcher (Producer):** The `producer.py` script, leveraging the `CTlogsStream` class from `stream.py`, connects to a specified CT log URL. It fetches batches of CT log entries and places them into a shared `multiprocessing.Queue`.

2.  **Certificate Processor (Consumer):** The `runner.py` script orchestrates multiple consumer processes, each running an instance of `CTLogsProcs` from `decrypt.py`. These consumers continuously pull log entry batches from the shared queue. For each entry:
    *   It decrypts and parses the raw CT log data to extract certificate information (e.g., issuer, subject, serial numbers, SANs, validity periods).
    *   The extracted, structured certificate data is then inserted into a ClickHouse database.

3.  **Monitoring & Visualization (Optional):**
    *   Grafana is included in the Docker Compose setup, allowing for potential visualization and dashboarding of the certificate data stored in ClickHouse.
    *   A basic `consumer.py` script is provided for monitoring Kafka topic messages, although Kafka is not currently used in the primary CT log processing pipeline.

## Architecture Diagram

```
+-------------------+      +-----------------------+     
| CT Log Source     |      | Producer (stream.py)  |
| (e.g., Nimbus 2025)+-----> (Fetches CT Logs)     +
+-------------------+      |                       |      
                           +-----------+-----------+      
                                       |                           
                                       v                           
                           +-----------------------+      +------------------+
                           | multiprocessing.Queue |      | ClickHouse DB    |
                           | (In-memory Buffer)    |<-----+ (certs table)    |
                           +-----------------------+      +------------------+
                                                                   ^
                                                                   |
                                                            +--------------+
                                                            | Grafana      |
                                                            | (Dashboards) |
                                                            +--------------+
```

## Technology Stack

*   **Python:** Core scripting language for producer and consumer logic.
*   **Libraries:**
    *   `requests`: For fetching data from CT log URLs.
    *   `certlib`, `OpenSSL`, `cryptography`: For certificate parsing and decryption.
    *   `clickhouse_connect`: Python client for interacting with ClickHouse.
    *   `multiprocessing`: For inter-process communication (shared queue) between producer and consumers.
*   **Databases/Messaging:**
    *   **ClickHouse:** An open-source, column-oriented database management system for online analytical processing (OLAP). Used for storing parsed certificate data.
    *   **Kafka / Zookeeper:** (Included in `docker-compose.yml` for infrastructure, but not actively used in the current data processing pipeline from CT log to ClickHouse. Intended for future use or alternative data ingestion.)
*   **Visualization:**
    *   **Grafana:** A leading open-source platform for monitoring and observability, used for creating dashboards to visualize the data in ClickHouse.
*   **Containerization:**
    *   **Docker / Docker Compose:** Used to containerize and orchestrate ClickHouse, Grafana, Kafka, and Zookeeper services.

## Datastream Rate Limiting Strategy

To ensure stable operation and prevent overloading external CT log servers or internal processing capabilities, this project employs a datastream rate limiting strategy, primarily on the producer side (`CTlogsStream` in `stream.py`). This strategy helps maintain a healthy balance between data ingestion speed and system resources.

### Producer-side Rate Limiting (Concurrency Control)

The `CTlogsStream` utilizes a dynamic concurrency control mechanism to limit the rate at which new CT log entries are fetched:
*   **Staggered Worker Launch:** New worker threads, responsible for fetching batches of CT logs, are launched with a calculated delay (`time.sleep(stagger)`). This `stagger` value is dynamically adjusted based on the number of currently active workers.
*   **Dynamic Staggering:** As the number of active workers increases, the delay before launching a new worker also increases. This prevents a sudden burst of requests to the CT log server and ensures that the system doesn't create more fetching threads than it can efficiently manage, thereby regulating the overall data ingestion rate.

This approach effectively limits the upstream pull rate, preventing potential resource exhaustion on the CT log source and ensuring that the `multiprocessing.Queue` (in-memory buffer) and downstream consumers have sufficient capacity to handle the incoming data without being overwhelmed.

## Setup and Running

### Security Considerations

It is highly recommended that you change any default or sample sensitive information, such as passwords, before deploying this application in a production environment.

**Specifically:**

*   **ClickHouse Password:** The default password for the `default` user is set to `mysecretpassword` in `docker-compose.yml` and `decrypt.py`. You should change this to a strong, unique password.
    *   **Action:** Modify the `CLICKHOUSE_PASSWORD` environment variable in `docker-compose.yml` and update the `password` argument in `clickhouse_connect.get_client` calls within `decrypt.py` to match.
*   **Grafana Admin Password:** The default administrator password for Grafana is `admin` in `docker-compose.yml`.
    *   **Action:** Modify the `GF_SECURITY_ADMIN_PASSWORD` environment variable in `docker-compose.yml`.

Always ensure that secrets and sensitive configurations are handled securely, especially in production deployments.

### 1. Prerequisites

*   **Docker and Docker Compose:** Ensure Docker and Docker Compose are installed on your system.

### 2. Start Services with Docker Compose

Navigate to the root directory of the repository and start the Docker services (ClickHouse, Grafana, Kafka, Zookeeper):

```bash
docker-compose up -d
```

This will start the necessary infrastructure in the background.

### 3. Install Python Dependencies

Install the required Python libraries using `pip`:

```bash
pip install -r requirements.txt
```

### 4. Run the CT Logs Processor

The `decrypt.py` script automatically initializes the `certs` table in ClickHouse upon its first execution (when `CTLogsProcs` is instantiated).

To start the producer and consumer processes, run the `runner.py` script. You can specify various arguments such as the CT log URL, index range, and number of consumers.

Example:

```bash
python runner.py --ctlog_url "https://ct.cloudflare.com/logs/nimbus2025/" --start_index 0 --end_index 100 --num_consumers 4
```

*   `--ctlog_url`: The URL of the Certificate Transparency log to fetch from.
*   `--start_index`: The starting index for fetching CT log entries.
*   `--end_index`: The ending index for fetching CT log entries.
*   `--num_consumers`: The number of parallel consumer processes to run.

The `runner.py` script includes a monitoring loop that prints progress to the console. You can stop it with `Ctrl+C`.

### 5. Access Grafana (Optional)

Once Grafana is running via Docker Compose, you can access its web interface at `http://localhost:3000`.
*   **Username:** `admin`
*   **Password:** `admin`

You will need to configure a data source in Grafana to connect to the ClickHouse database (host: `clickhouse`, port: `8123`, database: `certs_db`, user: `default`, password: `mysecretpassword`). After configuring the data source, you can create dashboards to visualize the certificate data.

