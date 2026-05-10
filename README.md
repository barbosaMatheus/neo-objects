# NEO Asteroid Data Pipeline

A portfolio project demonstrating a modern data engineering pipeline using Apache Airflow, PostgreSQL, and Docker. This pipeline processes Near-Earth Object (NEO) asteroid data through a medallion architecture, transforming raw CSV data into increasingly refined layers suitable for analytics and machine learning.

## Architecture

The pipeline implements a **medallion architecture** with four layers:

### Bronze Layer
- **Source**: Raw `neo.csv` file
- **Format**: Parquet files for efficient storage and querying
- **Purpose**: Immutable raw data landing zone
- **Table**: `bronze_neo` in PostgreSQL

### Silver Layer
- **Source**: Bronze parquet files
- **Transformations**: Data cleaning, type standardization, deduplication
- **Purpose**: Curated, cleaned data
- **Table**: `silver_neo` in PostgreSQL

### Gold Layer
- **Source**: Silver table
- **Transformations**: Business logic, aggregations, enrichment
- **Purpose**: Business-ready analytics data
- **Table**: `gold_neo` in PostgreSQL

### ML Gold Layer
- **Source**: Gold table
- **Transformations**: Feature engineering, statistical aggregations per asteroid
- **Purpose**: Machine learning-ready dataset with descriptive statistics
- **Table**: `ml_gold_neo` in PostgreSQL

## Technologies Used

- **Apache Airflow**: Workflow orchestration and scheduling
- **PostgreSQL**: Data storage and SQL transformations
- **Docker & Docker Compose**: Containerized deployment
- **Python**: Data processing with Pandas and PyArrow
- **Parquet**: Efficient columnar storage format

## Prerequisites

- Docker and Docker Compose installed
- At least 4GB RAM available for containers
- Basic familiarity with Docker and SQL

## Setup and Installation

1. **Clone or navigate to the project directory**:
   ```bash
   cd neo-objects
   ```

2. **Build and start the services**:
   ```bash
   docker compose up --build
   ```
   
   This will:
   - Start PostgreSQL databases (Airflow metadata and neo-db)
   - Start Redis for Celery
   - Start Airflow services (webserver, scheduler, worker)

3. **Wait for services to be healthy**:
   - The initial build may take 5-10 minutes
   - Check logs for "healthy" status messages

## Usage

### Accessing Airflow Web UI

1. Open your browser and navigate to: `http://localhost:8080`
2. Login with:
   - Username: `airflow`
   - Password: `airflow`

### Running the DAGs

DAGs should be run in the following order to maintain data dependencies:

1. **neo_medallion_pipeline** (Bronze Layer)
   - Processes raw CSV → Parquet → PostgreSQL bronze table
   - Run this first to establish the base data

2. **neo_silver_pipeline** (Silver Layer) *[Planned]*
   - Transforms bronze data into cleaned silver table
   - Run after bronze is complete

3. **neo_gold_pipeline** (Gold Layer) *[Planned]*
   - Creates business-ready gold table from silver
   - Run after silver is complete

4. **neo_ml_gold_pipeline** (ML Gold Layer) *[Planned]*
   - Generates ML-ready features with aggregations
   - Run after gold is complete

To run a DAG:
1. In Airflow UI, find the DAG in the list
2. Click the play button (▶️) to trigger it
3. Monitor progress in the Graph View and Logs

### Database Inspection

Connect to the neo-db PostgreSQL database to inspect the created tables:

```bash
# From project directory
docker compose exec neo-db psql -U neo -d neo_data
```

Useful commands:

```sql
-- List all tables
\dt

-- View bronze table structure
\d bronze_neo

-- Sample bronze data
SELECT * FROM bronze_neo LIMIT 10;

-- View silver table (when implemented)
\d silver_neo
SELECT * FROM silver_neo LIMIT 10;

-- View gold table (when implemented)
\d gold_neo
SELECT * FROM gold_neo LIMIT 10;

-- View ML gold table (when implemented)
\d ml_gold_neo
SELECT * FROM ml_gold_neo LIMIT 10;

-- Count records in each layer
SELECT 'bronze' as layer, COUNT(*) as records FROM bronze_neo
UNION ALL
SELECT 'silver', COUNT(*) FROM silver_neo
UNION ALL
SELECT 'gold', COUNT(*) FROM gold_neo
UNION ALL
SELECT 'ml_gold', COUNT(*) FROM ml_gold_neo;
```

## Data Flow

```
neo.csv → Bronze (Parquet + Postgres) → Silver (Cleaned) → Gold (Enriched) → ML Gold (Aggregated)
```

Each layer builds upon the previous, ensuring data quality increases while maintaining auditability.

## Development Notes

- DAGs are located in the `dags/` directory
- Data files are mounted to `/opt/airflow/data` in containers
- Environment variables are configured in `.env`
- Logs are available in the `logs/` directory

## Future Enhancements

- Implement silver, gold, and ML gold DAGs
- Add data quality checks and monitoring
- Implement incremental loading with upsert logic
- Add automated testing for DAGs
- Create dashboard visualizations for the processed data

## License

This project is for educational and portfolio purposes.