# 🎬 Movie Booking CDC Pipeline with Snowflake Dynamic Tables

A production-ready **Change Data Capture (CDC)** solution built with Snowflake's advanced features including **Streams**, **Tasks**, and **Dynamic Tables**. This project demonstrates real-time data processing for movie booking analytics with enhanced business logic, automated data quality checks, and comprehensive change tracking.

## 🎯 Overview

This project implements a **medallion architecture** (Bronze → Silver → Gold) for movie booking data processing using Snowflake's native CDC capabilities. The solution captures all data changes in real-time, applies business logic transformations, and generates actionable analytics automatically.

### Key Benefits

- ✅ **Real-time Change Tracking**: Automatic capture of all INSERT, UPDATE, DELETE operations.
- ✅ **Automated Processing**: Tasks and Dynamic Tables handle data flow automatically.
- ✅ **Business Logic Integration**: Built-in data categorization and quality scoring.
- ✅ **Scalable Architecture**: Designed for production workloads
- ✅ **Cost-Efficient**: Optimized refresh schedules and warehouse usage

## 🏗️ Architecture

### Medallion Architecture Layers

```
┌─────────────────────────────────────────────────────────────────┐
│                          RAW LAYER                               │
│  raw_movie_bookings (Source Table with Computed Columns)        │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│                    CHANGE DETECTION                              │
│     movie_bookings_stream (Captures all changes)                │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│                      BRONZE LAYER                                │
│  movie_booking_cdc_events (Raw CDC with Metadata)               │
│  - Change Action (INSERT/UPDATE/DELETE)                         │
│  - Is Update Flag                                               │
│  - Change Timestamp                                             │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│                      SILVER LAYER                                │
│  movie_bookings_filtered (Enhanced Dynamic Table)               │
│  - Business Categorizations                                     │
│  - Revenue Analysis Fields                                      │
│  - Data Quality Validation                                      │
│  - Time-based Derived Fields                                    │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│                       GOLD LAYER                                 │
│  movie_booking_insights (Aggregated Analytics)                  │
│  - KPI Metrics by Movie                                         │
│  - Revenue Analysis                                             │
│  - Booking Pattern Analytics                                    │
│  - Data Quality Scores                                          │
└─────────────────────────────────────────────────────────────────┘
```

### Component Overview

| Component | Type | Purpose | Refresh |
|-----------|------|---------|---------|
| `raw_movie_bookings` | Table | Source data with transactions | Manual/App |
| `movie_bookings_stream` | Stream | Captures all changes | Automatic |
| `movie_booking_cdc_events` | Table | Raw CDC events storage | 1 minute |
| `movie_bookings_filtered` | Dynamic Table | Enhanced business view | DOWNSTREAM |
| `movie_booking_insights` | Dynamic Table | Aggregated analytics | DOWNSTREAM |
| `consume_stream_task` | Task | Processes stream data | 1 minute |
| `refresh_movie_booking_insights` | Task | Refreshes analytics | 1 minute |

## ✨ Features

### 1. Real-time CDC Processing

- **Automatic Change Detection**: Snowflake Streams capture all data modifications
- **Complete Metadata**: Tracks operation type (INSERT/UPDATE/DELETE) and update flags
- **Timestamp Tracking**: Automatic created_at, updated_at, and change_timestamp
- **Zero Data Loss**: Every change preserved in CDC events table

### 2. Enhanced Business Logic

#### Status Categorization
```sql
ACTIVE (BOOKED) → Active revenue contributing bookings
INACTIVE (CANCELLED) → Lost revenue from cancellations
```

#### Booking Size Categories
```sql
SINGLE → 1 ticket
GROUP → 2-4 tickets
LARGE_GROUP → 5+ tickets
```

#### Price Categories
```sql
BUDGET → < $250
STANDARD → $250-$500
PREMIUM → > $500
```

### 3. Revenue Analysis

- **Active Revenue**: Revenue from confirmed (BOOKED) bookings
- **Lost Revenue**: Revenue from cancelled bookings
- **Gross Revenue**: Total booking value regardless of status
- **Average Revenue per Booking**: Calculated per movie and category

### 4. Data Quality Management

- **Automated Validation**: Built-in checks for null values, negative amounts, future dates
- **Quality Scoring**: Percentage of valid bookings tracked per movie
- **Invalid Record Filtering**: Automatic exclusion from analytics

### 5. Time-based Analytics

- Booking date extraction (date only)
- Hour of booking analysis
- Day of week patterns
- Distinct booking days count

## 🚀 Getting Started

### Prerequisites

- Snowflake account with appropriate privileges
- Access to create databases, schemas, tables, streams, tasks, and dynamic tables
- `COMPUTE_WH` warehouse or equivalent (configurable)
- Permissions to execute tasks and refresh dynamic tables .

## 📊 Database Schema

### Source Table: `raw_movie_bookings`

```sql
CREATE OR REPLACE TABLE raw_movie_bookings (
    booking_id STRING,                    -- Unique booking identifier
    customer_id STRING,                   -- Customer identifier  
    movie_id STRING,                      -- Movie identifier
    booking_date TIMESTAMP,               -- When booking was made
    status STRING,                        -- BOOKED, CANCELLED
    ticket_count INT,                     -- Number of tickets
    ticket_price NUMBER(10, 2),           -- Price per ticket
    total_amount NUMBER(10, 2) AS (ticket_count * ticket_price), -- Computed
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);
```

**Key Features:**
- Computed column `total_amount` for automatic calculation
- Default timestamps for audit tracking
- Simple status model (BOOKED/CANCELLED)

### CDC Events Table: `movie_booking_cdc_events`

```sql
CREATE OR REPLACE TABLE movie_booking_cdc_events (
    -- Original booking fields
    booking_id STRING,
    customer_id STRING,
    movie_id STRING,
    booking_date TIMESTAMP,
    status STRING,
    ticket_count INT,
    ticket_price NUMBER(10, 2),
    total_amount NUMBER(10, 2),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    -- CDC metadata
    change_action STRING,                 -- INSERT, UPDATE, DELETE
    is_update BOOLEAN,                    -- TRUE for updates
    change_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);
```

**Key Features:**
- Preserves complete original record
- Adds CDC metadata from stream
- Tracks exact change timestamp

### Enhanced Filtered Table: `movie_bookings_filtered`

Dynamic Table with derived business fields:

```sql
-- Business status categorization
booking_status_category → ACTIVE/INACTIVE

-- Size-based categorization
booking_size_category → SINGLE/GROUP/LARGE_GROUP

-- Price-based categorization
price_category → BUDGET/STANDARD/PREMIUM

-- Revenue analysis
active_revenue → Revenue from BOOKED bookings
lost_revenue → Revenue from CANCELLED bookings

-- Data quality
is_valid_booking → TRUE/FALSE based on validation rules
```

## 🔄 CDC Processing Logic

### Stream Processing Flow

1. **Change Capture**
   - Stream automatically detects changes to `raw_movie_bookings`
   - Captures INSERT, UPDATE, DELETE operations
   - Adds metadata columns: `METADATA$ACTION`, `METADATA$ISUPDATE`

2. **Task Processing** (`consume_stream_task`)
   - Runs every 1 minute
   - Reads new records from stream
   - Inserts into `movie_booking_cdc_events` with metadata
   - Stream automatically advances after successful processing

3. **Dynamic Table Refresh** (Silver Layer)
   - `movie_bookings_filtered` refreshes with TARGET_LAG = DOWNSTREAM
   - Applies business logic transformations
   - Filters invalid records
   - Calculates derived fields

4. **Analytics Aggregation** (Gold Layer)
   - `movie_booking_insights` refreshes after filtered table
   - Aggregates metrics by movie
   - Calculates KPIs and rates
   - Updates refresh timestamp.

## 📈 Analytics Capabilities

### Key Performance Indicators

#### Booking Metrics
- Total bookings count
- Valid bookings count
- Invalid bookings count
- New bookings count
- Status changes count
- Deleted bookings count

#### Revenue Metrics
- Total active revenue (BOOKED)
- Total lost revenue (CANCELLED)
- Total gross revenue
- Average revenue per active booking
- Revenue by price category
- Revenue by booking size

#### Operational Metrics
- Cancellation rate percentage
- Active booking rate percentage
- Data quality score
- Active booking days
- Active booking hours

## ⚙️ Configuration

### Task Configuration

```sql
-- Modify task frequency
ALTER TASK consume_stream_task 
SET SCHEDULE = '30 SECONDS';  -- More frequent

ALTER TASK consume_stream_task 
SET SCHEDULE = '5 MINUTE';    -- Less frequent

-- Change warehouse
ALTER TASK consume_stream_task 
SET WAREHOUSE = 'ANALYTICS_WH';

-- Suspend/Resume
ALTER TASK consume_stream_task SUSPEND;
ALTER TASK consume_stream_task RESUME;
```

### Dynamic Table Configuration

```sql
-- Modify refresh lag
ALTER DYNAMIC TABLE movie_bookings_filtered 
SET TARGET_LAG = '1 MINUTE';  -- More frequent

-- Manual refresh
ALTER DYNAMIC TABLE movie_bookings_filtered REFRESH;
ALTER DYNAMIC TABLE movie_booking_insights REFRESH;
```

### Warehouse Sizing

| Warehouse Size | Recommended For | Estimated Cost |
|----------------|-----------------|----------------|
| X-Small | Development/Testing | Lowest |
| Small | Production (< 1M records) | Low |
| Medium | Production (1M-10M records) | Medium |
| Large | Production (10M+ records) | High |

## 📊 Monitoring

### Task Monitoring

```sql
-- Check task execution history
SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    TASK_NAME => 'CONSUME_STREAM_TASK'
)) 
ORDER BY SCHEDULED_TIME DESC 
LIMIT 10;

-- Check task status
SHOW TASKS;
```

### Stream Monitoring

```sql
-- Check stream offset
SHOW STREAMS;

-- View pending changes
SELECT * FROM movie_bookings_stream;

-- Count pending changes
SELECT COUNT(*) as pending_changes 
FROM movie_bookings_stream;
```

### Dynamic Table Monitoring

```sql
-- Check refresh history
SELECT * FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY(
    TABLE_NAME => 'MOVIE_BOOKINGS_FILTERED'
)) 
ORDER BY REFRESH_START_TIME DESC 
LIMIT 5;

-- Check current lag
SHOW DYNAMIC TABLES;
```

### Data Quality Monitoring

```sql
-- Overall data quality
SELECT 
    COUNT(*) as total_records,
    SUM(CASE WHEN is_valid_booking THEN 1 ELSE 0 END) as valid_records,
    ROUND(SUM(CASE WHEN is_valid_booking THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as quality_score
FROM movie_bookings_filtered;

-- Quality by movie
SELECT 
    movie_id,
    data_quality_score,
    invalid_bookings
FROM movie_booking_insights
WHERE data_quality_score < 100.0
ORDER BY data_quality_score ASC;
```

See [MONITORING.md](docs/MONITORING.md) for comprehensive monitoring guide.

## 🎯 Best Practices

### Performance Optimization

1. **Warehouse Management**
   - Use auto-suspend (1-2 minutes)
   - Size appropriately for workload
   - Consider separate warehouses for tasks vs queries

2. **Task Scheduling**
   - Balance real-time needs with cost
   - Use longer intervals for lower-priority pipelines
   - Monitor execution times and adjust

3. **Dynamic Table Optimization**
   - Use DOWNSTREAM for dependent tables
   - Set appropriate target lag based on requirements
   - Monitor refresh times and resource usage

### Data Quality

1. **Validation Rules**
   - Implement comprehensive checks in filtered table
   - Monitor quality scores regularly
   - Alert on quality degradation

2. **Change Tracking**
   - Preserve complete history in CDC events
   - Regular archival of old CDC data
   - Implement retention policies

### Security

1. **Access Control**
   - Use role-based access control (RBAC)
   - Separate read/write permissions
   - Audit access logs regularly

2. **Data Privacy**
   - Consider PII data masking
   - Implement column-level security
   - Follow compliance requirements

## 🔧 Troubleshooting

### Common Issues

**Issue: Tasks not executing**
```sql
-- Check task status
SHOW TASKS;

-- Check if suspended
SELECT name, state, schedule 
FROM TABLE(INFORMATION_SCHEMA.TASKS)
WHERE name = 'CONSUME_STREAM_TASK';

-- Resume if needed
ALTER TASK consume_stream_task RESUME;
```

**Issue: Stream has no data**
```sql
-- Verify stream exists and is valid
SHOW STREAMS;

-- Check if source table has changes
SELECT COUNT(*) FROM raw_movie_bookings;

-- Recreate stream if needed
CREATE OR REPLACE STREAM movie_bookings_stream 
ON TABLE raw_movie_bookings;
```

**Issue: Dynamic tables not refreshing**
```sql
-- Check refresh history for errors
SELECT * FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY(
    TABLE_NAME => 'MOVIE_BOOKINGS_FILTERED'
)) 
WHERE STATE = 'FAILED'
ORDER BY REFRESH_START_TIME DESC;

-- Manual refresh to test
ALTER DYNAMIC TABLE movie_bookings_filtered REFRESH;
``` for detailed solutions.

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details. Contributors to this project

---

**Built with ❤️ using Snowflake CDC, Streams, Tasks, and Dynamic Tables**

**Last Updated:** December 2025
