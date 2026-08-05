# WHPG Real Streaming Demo

Real-time data streaming from Kafka into **WarehousePG** using **FlowServer** — with a live monitoring dashboard.

Two independent streams run in parallel:

| Stream | Kafka Topic | Format | Target Table | Rate |
|---|---|---|---|---|
| E-Commerce Orders | `ecommerce-orders` | JSON | `ecommerce_orders` | 50/s default |
| IoT Sensor Readings | `iot-sensors-csv` | CSV | `iot_sensor_readings` | 20/s default |

FlowServer applies SQL expressions at ingest time to compute derived columns — no ETL code required.

---

## Architecture

```
Go Generator → Kafka Topic → FlowServer → WarehousePG → Dashboard (Flask API)
```

```
WHPG-real-streaming/
├── configs/
│   └── flow_server.json          FlowServer daemon config (ports 6060/6070/9080)
├── sql/
│   ├── 01_create_database.sql    Create streaming_demo database
│   ├── 02_create_tables.sql      Create both tables with indexes
│   └── 03_verify_setup.sql       Verify setup and show row counts
├── generators/
│   ├── order-generator.go        Produces JSON orders to Kafka
│   ├── iot-generator.go          Produces CSV sensor readings to Kafka
│   └── go.mod
├── jobs/
│   ├── ecommerce-orders.yaml     FlowServer job — JSON → ecommerce_orders
│   └── iot-sensors-csv.yaml      FlowServer job — CSV → iot_sensor_readings
├── scripts/
│   ├── setup.sh                  One-time full setup
│   ├── start-demo.sh             Start all components
│   ├── stop-demo.sh              Stop all components
│   └── status-demo.sh            Check status of every component
└── dashboards/
    ├── api.py                    Unified Flask API (port 5055)
    └── dashboard.html            Live monitoring dashboard
```

---

## Prerequisites

| Component | Notes |
|---|---|
| WarehousePG | Running, accessible on port 5432 as `gpadmin` |
| Kafka | Running on `localhost:9092` |
| FlowServer | Binary `./flowserver` in repo root |
| `flowcli` | In `PATH` |
| Go 1.19+ | For building generators |
| Python 3.8+ | For the dashboard API |
| `kafka-topics` | Kafka CLI tools — expected at `/opt/kafka/bin` (added to `PATH` by scripts automatically) |

---

## Quick Start

### 1. Clone

```bash
git clone https://github.com/francksidi-edb/WHPG-real-streaming.git
cd WHPG-real-streaming
```

### 2. One-time setup

Runs database creation, table creation, Kafka topic creation, Go build, and pip install in one command:

```bash
./scripts/setup.sh
```

Options if your environment differs from defaults:

```bash
./scripts/setup.sh \
  --pg-host       localhost \
  --pg-port       5432 \
  --pg-user       gpadmin \
  --kafka         localhost:9092 \
  --kafka-user    kafka \
  --kafka-password your-password-here
```

This writes `configs/kafka-client.properties` (git-ignored) with SASL credentials and passes `--command-config` to every `kafka-topics` call automatically.

Or run each step manually:

```bash
# Database
psql -h localhost -p 5432 -U gpadmin -d postgres  -f sql/01_create_database.sql
psql -h localhost -p 5432 -U gpadmin -d streaming_demo -f sql/02_create_tables.sql

# Kafka topics — with SASL auth
kafka-topics --create --topic ecommerce-orders \
    --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 \
    --command-config configs/kafka-client.properties
kafka-topics --create --topic iot-sensors-csv \
    --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 \
    --command-config configs/kafka-client.properties

# Build generators
cd generators && go mod tidy
go build -o order-generator order-generator.go
go build -o iot-generator   iot-generator.go
cd ..

# Python dependencies
pip install flask flask-cors psycopg2-binary --break-system-packages
```

### Kafka Authentication

Kafka requires a username and password. Before running any `kafka-topics` command, create `configs/kafka-client.properties` with your credentials:

```bash
cat > configs/kafka-client.properties <<EOF
security.protocol=SASL_PLAINTEXT
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \
  username="kafka" \
  password="your-password-here";
EOF
```

A template is provided at `configs/kafka-client.properties.template`. The file is in `.gitignore` — credentials are never committed.

`setup.sh` creates this file automatically when you pass `--kafka-user` and `--kafka-password`. Every `kafka-topics` call in the scripts picks it up automatically via `--command-config`.

### 3. Start FlowServer

FlowServer must be running before jobs can be submitted or started:

```bash
./flowserver -c configs/flow_server.json &
```

Ports used:

| Port | Service |
|---|---|
| 6060 | FlowServer API |
| 6070 | Gpfdist |
| 9080 | Prometheus metrics (`/flow_metrics`) |
| 6080 | Debug |

### 4. Submit FlowServer jobs

Submit once per environment to register the jobs with FlowServer:

```bash
flowcli --server http://localhost:6060 job submit jobs/ecommerce-orders.yaml
flowcli --server http://localhost:6060 job submit jobs/iot-sensors-csv.yaml
```

### 5. Start the demo

```bash
./scripts/start-demo.sh
```

This starts: FlowServer jobs, both Go generators, and the dashboard API.

Custom rates:

```bash
./scripts/start-demo.sh --ecom-rate 100 --iot-rate 50
```

### 6. Open the dashboard

```
http://localhost:5055/
```

---

## Dashboard Tabs

| Tab | Description |
|---|---|
| **⚙ Control** | Start/stop FlowServer, jobs, and generators from the browser |
| **🗄 DB Setup** | All SQL scripts and setup commands with copy buttons |
| **📡 Live Streams** | Side-by-side live tickers for both topics, row counters |
| **🛒 E-Commerce** | Revenue KPIs, bucket charts, category bars, recent orders table |
| **🌡️ IoT Sensors** | Alert distribution, comfort index, building temps, per-sensor cards |
| **📋 Pipeline Config** | FlowServer YAML jobs and schema DDL with syntax highlighting |

---

## FlowServer Jobs

### ecommerce-orders — JSON ingestion

Reads JSON from `ecommerce-orders`, maps all fields, and computes three columns at ingest time:

```yaml
revenue_bucket:   CASE WHEN total_price < 100 THEN 'low'
                       WHEN total_price < 500 THEN 'medium'
                       WHEN total_price < 1000 THEN 'high'
                       ELSE 'premium' END

is_bulk_order:    quantity > 2

processing_time:  NOW()
```

### iot-sensors-csv — CSV ingestion

Reads CSV from `iot-sensors-csv` (skips header line), computes eight columns:

```yaml
alert_level:    0=normal  1=high_temp/low_temp/high_humidity  2=low_battery  3=other
building:       SPLIT_PART(location, '-', 1)
floor:          CAST(SPLIT_PART(location, '-', 3) AS INTEGER)
temperature_f:  (temperature * 9.0/5.0) + 32.0
comfort_index:  comfortable / acceptable / uncomfortable
battery_status: good / medium / low / critical
data_quality:   valid / suspicious
processed_at:   NOW()
```

---

## Managing the Demo

```bash
# Check status of every component
./scripts/status-demo.sh

# Stop everything
./scripts/stop-demo.sh

# Stop everything except FlowServer
./scripts/stop-demo.sh --keep-flowserver

# View live logs
tail -f /tmp/flowserver-demo/logs/*.log

# Restart just the generators at a new rate
./scripts/stop-demo.sh --keep-flowserver --keep-jobs
./scripts/start-demo.sh --no-flowserver --no-jobs --ecom-rate 200 --iot-rate 100
```

### Via the dashboard Control tab

All of the above is also available in the browser:

- **FlowServer** — Start / Stop / Show log
- **Jobs** — Submit / Start / Stop / Restart each job individually
- **Generators** — Set rate and max, start/stop each independently

---

## CLI Job Control

```bash
# List jobs
flowcli --server http://localhost:6060 job list

# Start / stop individual jobs
flowcli --server http://localhost:6060 job start ecommerce-orders
flowcli --server http://localhost:6060 job stop  iot-sensors-csv

# Check status
flowcli --server http://localhost:6060 job status ecommerce-orders

# Prometheus metrics
curl http://localhost:9080/flow_metrics
```

---

## Database

**Database:** `streaming_demo`  
**User:** `gpadmin`

### ecommerce_orders

| Column | Type | Source |
|---|---|---|
| order_id | VARCHAR(50) PK | Kafka JSON |
| timestamp | TIMESTAMP | Kafka JSON |
| customer_id / name / email | VARCHAR | Kafka JSON |
| product_id / name / category | VARCHAR | Kafka JSON |
| quantity, unit_price, total_price | NUMERIC | Kafka JSON |
| payment_method, country, city | VARCHAR | Kafka JSON |
| **revenue_bucket** | VARCHAR(20) | **FlowServer computed** |
| **is_bulk_order** | BOOLEAN | **FlowServer computed** |
| **processing_time** | TIMESTAMP | **FlowServer computed** |

### iot_sensor_readings

| Column | Type | Source |
|---|---|---|
| timestamp, sensor_id, location | — | Kafka CSV |
| temperature, humidity, pressure, battery_level | DECIMAL | Kafka CSV |
| status | VARCHAR | Kafka CSV |
| **alert_level** | INTEGER | **FlowServer computed** |
| **building** | VARCHAR | **FlowServer computed** |
| **floor** | INTEGER | **FlowServer computed** |
| **temperature_f** | DECIMAL | **FlowServer computed** |
| **comfort_index** | VARCHAR | **FlowServer computed** |
| **battery_status** | VARCHAR | **FlowServer computed** |
| **data_quality** | VARCHAR | **FlowServer computed** |
| **processed_at** | TIMESTAMP | **FlowServer computed** |

---

## Ports Reference

| Port | Service |
|---|---|
| 5432 | WarehousePG |
| 9092 | Kafka |
| 6060 | FlowServer API |
| 6070 | FlowServer Gpfdist |
| 9080 | FlowServer Prometheus |
| 6080 | FlowServer Debug |
| 5055 | Dashboard API + UI |
| `/tmp/flowserver-demo/` | PID files and logs |
