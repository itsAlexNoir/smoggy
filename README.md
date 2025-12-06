# smoggy

A comprehensive ETL (Extract, Transform, Load) pipeline that aggregates air quality, weather, traffic, and calendar data from open data sources to enable analysis and prediction of air quality in Madrid.

## Overview

Smoggy processes open data from Madrid City Hall to create a unified MongoDB database containing:
- **Air Quality Data**: PM₁₀, PM₂.₅, and other pollutant measurements
- **Weather Data**: Temperature, humidity, wind conditions
- **Traffic Data**: Vehicle density and traffic measurements
- **Calendar Data**: Temporal information (day, month, year)

The processed data enables deep learning models for air quality prediction and analysis.

## Project Structure

```
smoggy/
├── bin/                          # Startup scripts
│   ├── start_smoggydb.sh        # Start MongoDB daemon for main database
│   └── start_tiny_smoggydb.sh   # Start MongoDB daemon for testing database
├── etl/                         # ETL pipeline modules
│   ├── etl_weather.py           # Weather data extraction and loading
│   ├── etl_traffic.py           # Traffic data extraction and loading
│   ├── etl_calendar.py          # Calendar data extraction and loading
│   ├── request_weather_data.py  # Weather API data requests
│   └── tools/
│       ├── database.py          # MongoDB connection and operations
│       └── etl_utils.py         # Utility functions for ETL processes
├── tools/                       # General utility modules
│   ├── database.py              # MongoDB client utilities
│   ├── dataclean.py             # Data cleaning and transformation routines
│   └── aemet-api-key            # Spanish meteorological agency API key
├── notebooks/                   # Jupyter notebooks for analysis
│   ├── 1_analisis_calidad_aire.ipynb     # Air quality analysis
│   ├── get_pollution_station_info.ipynb  # Pollution station information
│   └── pollution_viewer.ipynb            # Interactive pollution visualization
├── LICENSE                      # GNU General Public License v3
└── README.md                    # This file
```

## Components

### ETL Pipeline (etl/)

#### Weather Data (`etl_weather.py`)
- Loads weather data from external sources
- Processes meteorological station information
- Stores climate data (temperature, humidity, wind) in MongoDB
- Supports data from multiple weather stations

#### Traffic Data (`etl_traffic.py`)
- Processes traffic measurement device (PMED) data
- Extracts traffic density information
- Parses CSV files with traffic measurements
- Handles location-based traffic data

#### Calendar Data (`etl_calendar.py`)
- Processes temporal calendar information
- Extracts date components (day, month, year)
- Enriches data with temporal features for time-based analysis

#### Weather API Requests (`request_weather_data.py`)
- Manages API calls to weather data sources
- Handles authentication with AEMET (Spanish meteorological agency)
- Retrieves real-time and historical weather data

### Database Module (`tools/database.py`)

Core MongoDB utilities:
- `connect_mongo_daemon()`: Establish connection to MongoDB server
- `get_mongo_database()`: Access specific database
- `get_mongo_collection()`: Access collection within database
- `insert_one_document()`: Insert single document
- `insert_many_documents()`: Insert multiple documents in batch

### Data Cleaning (`tools/dataclean.py`)

Utilities for cleaning and transforming raw data files:
- Text file parsing and validation
- Data format standardization
- Missing value handling
- Data type conversion

## Getting Started

### Prerequisites

- Python 3.x
- MongoDB
- Required Python packages:
  - `pymongo` - MongoDB Python driver
  - `pandas` - Data manipulation
  - `numpy` - Numerical computing
  - `absl-py` - Command-line flags and logging

### Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd smoggy
```

2. Install dependencies:
```bash
pip install pymongo pandas numpy absl-py
```

3. Configure MongoDB connection parameters in your environment or modify database.py as needed

### Running the ETL Pipeline

#### Start MongoDB Server

```bash
# For main database
./bin/start_smoggydb.sh

# For testing/development
./bin/start_tiny_smoggydb.sh
```

#### Running Individual ETL Jobs

Each ETL module supports command-line flags for configuration:

```bash
python etl/etl_weather.py --source_path=/path/to/weather/data
python etl/etl_traffic.py --source_path=/path/to/traffic/data
python etl/etl_calendar.py --source_path=/path/to/calendar/data
```

### Jupyter Notebooks

Three analysis notebooks are provided:

1. **1_analisis_calidad_aire.ipynb** - Comprehensive air quality analysis
2. **get_pollution_station_info.ipynb** - Explore pollution measurement stations
3. **pollution_viewer.ipynb** - Interactive visualization of pollution data

Launch Jupyter and open these notebooks for exploratory data analysis.

## Data Sources

The project processes open data from Madrid City Hall including:
- Air quality measurements from monitoring stations
- Traffic density from traffic measurement devices (PMED)
- Weather data from meteorological stations
- Calendar and temporal information

## Database Schema

The MongoDB database ("aire") contains collections for:
- **weather**: Meteorological measurements
- **traffic**: Traffic density and vehicle count data
- **air_quality**: Pollutant concentrations (PM₁₀, PM₂.₅, etc.)
- **calendar**: Temporal features and dates

## API Keys

The project includes support for AEMET (Spanish meteorological agency) API:
- API key location: `tools/aemet-api-key`
- Used by `request_weather_data.py` for weather data retrieval

## Author

**Alejandro de la Calle**
- Email: alejandrodelacallenegro@gmail.com

## License

This project is licensed under the GNU General Public License v3 (GPL-3.0).
See the [LICENSE](LICENSE) file for details.

## Status

**Development** - This project is actively under development.

## Contributing

Contributions are welcome. Please ensure code follows the project conventions and includes appropriate documentation.

## Notes

- All data loading operations interact with MongoDB
- The project uses Python's `absl` library for command-line interface and logging
- Data files are expected in CSV format with Latin-1 encoding
- Monitor database disk space as air quality data can be voluminous over extended periods
