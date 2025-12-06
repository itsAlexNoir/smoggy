# Smoggy Architecture

## System Overview

Smoggy is an ETL (Extract, Transform, Load) system designed to aggregate multi-source environmental data into a MongoDB database for air quality analysis and prediction.

```
┌─────────────────────────────────────────────────────────────┐
│                    Data Sources                              │
├─────────────────────────────────────────────────────────────┤
│  • Madrid Open Data Portal                                   │
│  • AEMET (Spanish Meteorological Agency)                     │
│  • Traffic Measurement Devices (PMED)                        │
│  • Weather Stations                                          │
└────────────────┬────────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────────┐
│              ETL Pipeline Layer (etl/)                       │
├─────────────────────────────────────────────────────────────┤
│  ┌──────────────────────────────────────────────────────┐  │
│  │ etl_weather.py                                       │  │
│  │ • Extract meteorological data                        │  │
│  │ • Transform temperature, humidity, wind data         │  │
│  │ • Load to weather collection                         │  │
│  └──────────────────────────────────────────────────────┘  │
│                                                              │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ etl_traffic.py                                       │  │
│  │ • Extract PMED traffic measurements                  │  │
│  │ • Parse traffic density data                         │  │
│  │ • Load to traffic collection                         │  │
│  └──────────────────────────────────────────────────────┘  │
│                                                              │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ etl_calendar.py                                      │  │
│  │ • Extract calendar information                       │  │
│  │ • Transform date features                            │  │
│  │ • Load temporal data                                 │  │
│  └──────────────────────────────────────────────────────┘  │
│                                                              │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ request_weather_data.py                              │  │
│  │ • Handle weather API calls                           │  │
│  │ • Manage AEMET authentication                        │  │
│  │ • Orchestrate data retrieval                         │  │
│  └──────────────────────────────────────────────────────┘  │
└────────────────┬────────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────────┐
│          Support/Utilities Layer (tools/)                   │
├─────────────────────────────────────────────────────────────┤
│  ┌──────────────────────────────────────────────────────┐  │
│  │ database.py                                          │  │
│  │ • MongoDB connection management                      │  │
│  │ • CRUD operations wrapper                            │  │
│  │ • Collection access utilities                        │  │
│  └──────────────────────────────────────────────────────┘  │
│                                                              │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ dataclean.py                                         │  │
│  │ • CSV parsing (Latin-1 encoding)                     │  │
│  │ • Data validation and cleaning                       │  │
│  │ • Format standardization                             │  │
│  │ • Missing value handling                             │  │
│  └──────────────────────────────────────────────────────┘  │
│                                                              │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ etl_utils.py                                         │  │
│  │ • Common ETL utilities                               │  │
│  │ • Data transformation helpers                        │  │
│  └──────────────────────────────────────────────────────┘  │
└────────────────┬────────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────────┐
│              MongoDB Database (aire)                         │
├─────────────────────────────────────────────────────────────┤
│  • weather collection        - Meteorological data           │
│  • traffic collection        - Traffic measurements          │
│  • air_quality collection    - Pollutant concentrations     │
│  • calendar collection       - Temporal features             │
└─────────────────────────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────────┐
│          Analysis & Visualization Layer                      │
├─────────────────────────────────────────────────────────────┤
│  • Jupyter Notebooks for exploratory analysis                │
│  • Python scripts for model development                      │
│  • Data visualization tools                                  │
└─────────────────────────────────────────────────────────────┘
```

## Data Flow

### ETL Pipeline Execution

1. **Extract**: Data is retrieved from external sources (CSV files, APIs)
2. **Transform**: Data is cleaned, validated, and formatted
3. **Load**: Processed data is inserted into MongoDB collections

### Example: Weather Data Pipeline

```
Weather CSV File
     │
     ▼
etl_weather.py
     │
     ├─ Read estaciones_meteo.json
     ├─ Parse weather measurements
     ├─ Validate data integrity
     │
     ▼
tools/dataclean.py
     │
     ├─ Standardize formats
     ├─ Handle missing values
     │
     ▼
tools/database.py
     │
     ├─ Connect to MongoDB
     ├─ Get/create weather collection
     ├─ Insert documents
     │
     ▼
MongoDB weather collection
```

## Database Schema

### Weather Collection
```python
{
    "_id": ObjectId,
    "station_id": string,
    "station_name": string,
    "temperature": float,
    "humidity": float,
    "wind_speed": float,
    "wind_direction": string,
    "timestamp": datetime,
    "location": {
        "latitude": float,
        "longitude": float
    }
}
```

### Traffic Collection
```python
{
    "_id": ObjectId,
    "pmed_id": string,
    "location": string,
    "vehicle_count": integer,
    "vehicle_density": float,
    "timestamp": datetime,
    "district": string,
    "intensity": integer
}
```

### Air Quality Collection
```python
{
    "_id": ObjectId,
    "station_id": string,
    "station_name": string,
    "pm10": float,
    "pm25": float,
    "no2": float,
    "o3": float,
    "timestamp": datetime,
    "location": {
        "latitude": float,
        "longitude": float
    }
}
```

### Calendar Collection
```python
{
    "_id": ObjectId,
    "date": datetime,
    "day": integer,
    "month": integer,
    "year": integer,
    "day_of_week": integer,
    "is_weekend": boolean,
    "is_holiday": boolean
}
```

## Key Components

### MongoDB Module (`tools/database.py`)

**Connection Management**:
- `connect_mongo_daemon()`: Creates MongoDB client connection
- Supports custom host and port configuration
- Default: localhost:27017

**Database Operations**:
- `get_mongo_database()`: Access database by name
- `get_mongo_collection()`: Access collection within database
- `insert_one_document()`: Insert single document
- `insert_many_documents()`: Batch insert with optimization

### Data Cleaning Module (`tools/dataclean.py`)

Handles:
- Text file parsing with Latin-1 encoding (default for Spanish data)
- CSV delimiter handling (typically semicolon)
- Data type conversion
- Missing value detection and handling
- Duplicate record removal
- Outlier detection and handling

### ETL Modules

**Common Pattern**:
```python
def main(argv):
    # 1. Parse command-line arguments
    # 2. Log operation start
    # 3. Connect to MongoDB
    # 4. Load/create collections
    # 5. Read and parse source data
    # 6. Transform and validate data
    # 7. Insert into MongoDB
    # 8. Log completion and statistics
```

## Technology Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| Database | MongoDB | Document storage for flexible schema |
| Data Processing | Pandas | DataFrames for data transformation |
| Numerical Computing | NumPy | Array operations and calculations |
| CLI/Logging | absl-py | Command-line interface and logging |
| Analysis | Jupyter | Interactive data exploration |
| Language | Python 3.x | Core implementation language |

## Deployment Architecture

### Development Environment
- Single MongoDB instance on local machine or external drive
- Direct Python script execution
- Jupyter notebooks for analysis

### Production Considerations
- MongoDB replication for data redundancy
- Database snapshots for backup
- Scheduled ETL job execution (cron/scheduler)
- Data retention policies

## Performance Considerations

### Database Optimization
- Indexing on frequently queried fields (station_id, timestamp)
- TTL indexes for data retention policies
- Connection pooling for batch operations

### ETL Performance
- Batch insertion using `insert_many_documents()`
- Streaming large datasets instead of loading into memory
- Parallel processing of independent data sources

### Scalability
- MongoDB sharding for large datasets
- Data archiving and partitioning by date
- API rate limiting to avoid overwhelming sources

## Error Handling & Logging

All modules use `absl.logging` for consistent logging:
- **INFO**: Operation progress and key milestones
- **ERROR**: Failed operations and data validation errors
- **WARNING**: Data quality issues and missing values

Exit codes:
- `0`: Successful completion
- `1`: Configuration error (missing arguments)
- `2`: Data processing error
- `3`: Database connection error

## Extension Points

### Adding New Data Sources
1. Create new ETL module in `etl/` directory
2. Follow existing patterns for data extraction and transformation
3. Use `tools/database.py` for MongoDB operations
4. Add corresponding collection definition to database schema
5. Update documentation and notebooks

### Adding New Analysis
1. Create Jupyter notebook in `notebooks/` directory
2. Use provided utilities for data access
3. Document methodology and findings
4. Add generated insights to README

## Security Considerations

- API keys stored in separate files (e.g., `tools/aemet-api-key`)
- MongoDB authentication configured via connection parameters
- Data source validation and sanitization
- No hardcoded credentials in scripts
- Access control through MongoDB user roles (production)

## Future Enhancements

- Real-time data streaming pipeline
- Machine learning model integration
- REST API for data access
- Dashboard for visualization
- Automated data quality monitoring
- Data warehouse integration (Snowflake, BigQuery)
