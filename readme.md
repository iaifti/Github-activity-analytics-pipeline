# GitHub Activity Analytics Pipeline

## Overview
Production-grade data pipeline extracting GitHub repository events, orchestrating ETL workflows with Apache Airflow, and transforming data with dbt for behavioral analytics.

## Problem Statement
Open source maintainers and data teams lack real-time visibility into repository health metrics. This pipeline provides automated, scalable analytics on:
- Commit velocity and contributor activity
- Issue and PR trends
- Community engagement (stars, forks, watches)
- Contributor retention and project momentum

## Architecture
Coming soon..

### Data Flow
1. **Extraction**: Airflow scheduler triggers Python script every 6 hours
2. **API Handling**: Python requests library with retry logic, rate limit management
3. **Data Lake**: Raw JSON stored in S3 with date partitioning (year/month/day)
4. **Warehouse Loading**: Snowflake COPY INTO from S3 external stage
5. **Transformation**: dbt models flatten JSON and aggregate metrics
6. **Analytics**: Pre-computed tables for dashboards and ad-hoc queries

## Technologies

### Orchestration & Compute
- **Apache Airflow 2.7** - Workflow orchestration with LocalExecutor
- **Python 3.10** - Extraction logic with requests, boto3, snowflake-connector
- **Docker Compose** - Containerized Airflow deployment

### Storage & Warehousing
- **AWS S3** - Data lake for raw event storage (~500KB/day)
- **Snowflake** - Cloud data warehouse (X-SMALL warehouse)

### Transformation & Analytics
- **dbt Core** - SQL transformations with testing and documentation
- **SQL** - Advanced analytics (CTEs, window functions, aggregations)

### Monitoring
- **Airflow UI** - DAG execution monitoring, task logs
- **dbt docs** - Data lineage and model documentation
- **AWS CloudWatch** - S3 access logs

## Data Model

### Raw Layer
- `raw_github_events` (Snowflake table)
  - Grain: One row per GitHub event
  - Format: VARIANT (JSON) from S3
  - Partitioning: S3 date partitioning (year/month/day)

### Staging Layer (dbt)
- `stg_github_events`
  - Flattened JSON into relational columns
  - Parsed event-type-specific payload fields
  - Standardized timestamps and identifiers

### Analytics Layer (dbt)
- `agg_daily_repo_activity`
  - Grain: One row per repository per day
  - Metrics: Total events, contributors, commits, PRs, issues
  - Features: 7-day rolling average, daily rank

- `agg_top_contributors`
  - Grain: One row per contributor per repository
  - Metrics: Contribution score (weighted), activity duration
  - Ranking: Top 50 contributors per repo

- `agg_event_trends`
  - Grain: One row per repo per event type per hour
  - Metrics: Event count, unique users, hour-over-hour change

## Key Technical Implementations

### API Rate Limit Handling
```python
if response.status_code == 403:
    reset_time = int(response.headers.get('X-RateLimit-Reset'))
    wait_seconds = max(reset_time - int(time.time()), 0) + 10
    time.sleep(wait_seconds)
```
Detects GitHub rate limit (403), calculates wait time from reset timestamp, sleeps until limit refreshes.

### Incremental Data Loading
S3 partitioning by date enables:
- Only loading new files (not re-processing historical data)
- Cost optimization (query only relevant partitions)
- Parallel processing potential (partition pruning)

### dbt Testing Strategy
- **Schema tests**: Uniqueness, not-null, referential integrity
- **Accepted values**: Event types validation
- **Row count checks**: Data completeness validation

### Airflow Task Dependencies
```python
extract_task >> validate_task >> load_task >> dbt_run >> dbt_test
```
- Sequential execution with XCom metadata passing
- Automatic retries (2 attempts, 5-min delay)
- Failure alerting via task status

## Sample Analytical Queries

### Repository Health Over Time
```sql
SELECT 
    repo_name,
    activity_date,
    total_events,
    unique_contributors,
    week_avg,
    (total_events - week_avg) / week_avg * 100 as pct_above_avg
FROM agg_daily_repo_activity
WHERE repo_name = 'apache/airflow'
    AND activity_date >= CURRENT_DATE - 30
ORDER BY activity_date DESC;
```

**Insight Example**: Apache Airflow showed 23% above-average activity during release weeks, with contributor count increasing 40% month-over-month.

### Top Contributors by Weighted Score
```sql
SELECT 
    contributor,
    repo_name,
    commit_count,
    pr_count,
    contribution_score,
    contributor_rank
FROM agg_top_contributors
WHERE repo_name = 'dbt-labs/dbt-core'
    AND contributor_rank <= 10
ORDER BY contribution_score DESC;
```

### Event Trend Detection (Anomalies)
```sql
SELECT 
    repo_name,
    event_type,
    event_hour,
    event_count,
    hour_over_hour_change
FROM agg_event_trends
WHERE abs(hour_over_hour_change) > 50  -- 50+ event spike
    AND event_hour >= CURRENT_TIMESTAMP - INTERVAL '24 hours'
ORDER BY abs(hour_over_hour_change) DESC;
```

## Running This Project

### Prerequisites
- AWS Account (free tier sufficient)
- Snowflake Account (trial or paid)
- Docker Desktop installed
- GitHub Personal Access Token

### Setup Instructions

**1. Clone Repository**
```bash
git clone https://github.com/iaifti/github-analytics-pipeline
cd github-analytics-pipeline
```

**2. Configure AWS**
```bash
aws configure
# Enter access key, secret key, region (us-east-1)
```

**3. Create S3 Bucket**
```bash
aws s3 mb s3://github-pipeline-examplename-12345...
```

**4. Set Environment Variables**
```bash
export AWS_ACCESS_KEY_ID=your_key
export AWS_SECRET_ACCESS_KEY=your_secret
export GITHUB_TOKEN=your_github_token
export SNOWFLAKE_USER=your_user
export SNOWFLAKE_PASSWORD=your_password
export SNOWFLAKE_ACCOUNT=xy12345.snowflake.com
```

**5. Start Airflow**
```bash
docker-compose up -d
```

**6. Access Airflow UI**
- Navigate to http://localhost:8080
- Login: admin / admin
- Unpause `github_events_pipeline` DAG

**7. Run dbt Models**
```bash
cd dbt_project
dbt run
dbt test
dbt docs generate
dbt docs serve
```

## Monitoring & Maintenance

### Pipeline Health Checks
- **Airflow**: DAG success rate (target: >95%)
- **Data Quality**: dbt test pass rate (target: 100%)
- **Latency**: Extraction to analytics availability (<15 minutes)
- **Costs**: AWS S3 + Snowflake (~$5-10/month)

### Alerting
- Airflow task failures → Check logs in UI
- dbt test failures → Review failing test query
- API rate limits → Logged in Airflow task output

### Scaling Considerations
- **More repositories**: Add to REPOS list (current: 2)
- **Higher frequency**: Change schedule to hourly (impact: 4x API calls)
- **Larger data**: Implement dbt incremental models
- **Distributed processing**: Upgrade to CeleryExecutor

## Skills Demonstrated

### Data Engineering
- REST API integration with error handling
- Event-driven pipeline orchestration
- Cloud data lake design (S3 partitioning)
- Data warehouse loading strategies
- ETL to ELT architecture transition

### Python
- Production-grade error handling (retries, timeouts)
- API rate limit management
- AWS SDK (boto3) for S3 operations
- Snowflake connector for warehouse loading

### Apache Airflow
- DAG design with task dependencies
- PythonOperator and BashOperator usage
- XCom for inter-task communication
- Scheduling and retry configuration
- Docker Compose deployment

### dbt & SQL
- Dimensional modeling concepts
- JSON parsing and flattening (Snowflake VARIANT type)
- Window functions (LAG, RANK, rolling averages)
- Complex aggregations and CTEs
- Data quality testing

### Cloud (AWS)
- S3 bucket configuration and IAM policies
- Storage integration with Snowflake
- Cost optimization (free tier usage)

## Cost Breakdown

**Monthly Operating Cost: ~$8-12**

- AWS S3: ~$0.50/month (10GB storage)
- Snowflake: ~$5-8/month (X-SMALL warehouse, ~2 hours compute/month)
