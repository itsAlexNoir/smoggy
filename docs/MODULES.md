# Module Documentation

Detailed documentation for each module in the Smoggy project.

## Table of Contents

- [ETL Modules](#etl-modules)
- [Tools Modules](#tools-modules)
- [Common Patterns](#common-patterns)
- [Code Examples](#code-examples)

## ETL Modules

### etl_weather.py

**Purpose**: Extract and load weather/meteorological data into MongoDB

**Key Functions**:

#### `define_flags()`
Defines command-line arguments for the module.

```python
--source_path: str (required)
    Path to weather source data directory
```

#### `get_weather_stations(source_path)`
Loads meteorological station information from JSON file.

**Parameters**:
- `source_path` (str): Path containing `estaciones_meteo.json`

**Returns**:
- `dict`: Station information with station_id, name, location

**Example**:
```python
stations = get_weather_stations('/data/weather')
# Returns: {'estacion_001': {'nombre': 'Madrid Centro', 'lat': 40.42, 'lon': -3.70}}
```

#### `main(argv)`
Main entry point for weather ETL execution.

**Flow**:
1. Connect to MongoDB
2. Create/access weather collection
3. Read station information
4. Parse temperature, humidity, wind data
5. Insert documents into weather collection

**Usage**:
```bash
python etl/etl_weather.py --source_path=/data/weather
```

---

### etl_traffic.py

**Purpose**: Extract and load traffic density and measurement data

**Key Functions**:

#### `get_pmed_dataframes_from_paths(path)`
Loads traffic measurement device (PMED) data from CSV files.

**Parameters**:
- `path` (str): Directory containing pmed_*.csv files

**Returns**:
- `list[pd.DataFrame]`: List of DataFrames, one per PMED station

**Example**:
```python
pmed_dfs = get_pmed_dataframes_from_paths('/data/traffic')
# Each DataFrame contains columns: intensity, occupancy, load, speed, etc.
```

**Note**: Files use semicolon delimiter and Latin-1 encoding

#### `get_traffic_density_dataframes_from_paths(path)`
Loads traffic density measurement data from CSV files.

**Parameters**:
- `path` (str): Directory containing density CSV files

**Returns**:
- `list[pd.DataFrame]`: DataFrames with traffic density information

#### `get_location_for_pmeds(dfs)`
Extracts location information from PMED DataFrames.

**Parameters**:
- `dfs` (list[pd.DataFrame]): PMED DataFrames

**Returns**:
- `list[dict]`: Location metadata for each PMED station

#### `main(argv)`
Main entry point for traffic ETL execution.

**Flow**:
1. Load PMED and density DataFrames
2. Extract location information
3. Transform data to document format
4. Insert into traffic collection

**Usage**:
```bash
python etl/etl_traffic.py --source_path=/data/traffic
```

---

### etl_calendar.py

**Purpose**: Extract and enrich temporal/calendar data

**Key Functions**:

#### `define_flags()`
Defines command-line arguments.

```python
--source_path: str (required)
    Path to calendar source data directory
```

#### `get_calendar_from_source(source_path)`
Loads and processes calendar data from CSV.

**Parameters**:
- `source_path` (str): Directory containing `calendario.csv`

**Returns**:
- `list[dict]`: List of calendar documents with date components

**Data Extraction**:
- `Dia`: Raw date string (DD/MM/YYYY format)
- `Fecha`: Parsed datetime object
- `Día`: Day of month (1-31)
- `Mes`: Month (1-12)
- `Año`: Year (YYYY)

**Example**:
```python
calendar_docs = get_calendar_from_source('/data/calendar')
# Returns: [
#     {'Día': 1, 'Mes': 1, 'Año': 2024},
#     {'Día': 2, 'Mes': 1, 'Año': 2024},
#     ...
# ]
```

#### `main(argv)`
Main entry point for calendar ETL execution.

**Flow**:
1. Connect to MongoDB
2. Load calendar data from CSV
3. Transform and extract temporal features
4. Insert into calendar collection

**Usage**:
```bash
python etl/etl_calendar.py --source_path=/data/calendar
```

---

### request_weather_data.py

**Purpose**: Request and retrieve weather data from external APIs

**Features**:
- AEMET (Spanish meteorological agency) API integration
- API key management and authentication
- Data retrieval orchestration

**API Key Configuration**:
- Location: `tools/aemet-api-key`
- Format: Plain text API key (one per line)

**Note**: This module coordinates weather data requests before passing to `etl_weather.py`

---

## Tools Modules

### tools/database.py

**Purpose**: MongoDB connection management and CRUD operations

**Key Functions**:

#### `connect_mongo_daemon(host=None, port=None)`
Establishes connection to MongoDB server.

**Parameters**:
- `host` (str, optional): MongoDB hostname (default: localhost)
- `port` (int, optional): MongoDB port (default: 27017)

**Returns**:
- `MongoClient`: Connected MongoDB client

**Example**:
```python
from tools.database import connect_mongo_daemon

client = connect_mongo_daemon()
# or with custom host/port:
client = connect_mongo_daemon('mongodb.example.com', 27017)
```

#### `get_mongo_database(client, dbname)`
Access specific database from MongoDB client.

**Parameters**:
- `client` (MongoClient): Connected MongoDB client
- `dbname` (str): Database name

**Returns**:
- `Database`: Database object

**Example**:
```python
db = get_mongo_database(client, 'aire')
```

#### `get_mongo_collection(db, collection)`
Access specific collection within database.

**Parameters**:
- `db` (Database): Database object
- `collection` (str): Collection name

**Returns**:
- `Collection`: Collection object for document operations

**Example**:
```python
weather_collection = get_mongo_collection(db, 'weather')
```

#### `insert_one_document(db, coll, entry)`
Insert single document into collection.

**Parameters**:
- `db` (Database): Database object
- `coll` (str): Collection name
- `entry` (dict): Document to insert

**Returns**:
- `ObjectId`: ID of inserted document

**Example**:
```python
doc_id = insert_one_document(db, 'weather', {
    'station_id': 'estacion_001',
    'temperature': 22.5,
    'timestamp': datetime.now()
})
```

#### `insert_many_documents(db, coll, entries)`
Insert multiple documents into collection (batch operation).

**Parameters**:
- `db` (Database): Database object
- `coll` (str): Collection name
- `entries` (list[dict]): Documents to insert

**Returns**:
- `list[ObjectId]`: IDs of inserted documents

**Example**:
```python
docs = [
    {'station_id': 's001', 'temperature': 22.5},
    {'station_id': 's002', 'temperature': 21.3},
]
doc_ids = insert_many_documents(db, 'weather', docs)
print(f"Inserted {len(doc_ids)} documents")
```

**Performance**: Batch insertion significantly faster than individual inserts

---

### tools/dataclean.py

**Purpose**: Data cleaning, validation, and transformation utilities

**Key Functions**:

#### Data Parsing
- CSV file parsing with configurable delimiters
- Latin-1 encoding support for Spanish data
- DataFrame to dictionary conversion

#### Data Cleaning
- Missing value detection and handling
- Duplicate record identification
- Data type conversion validation
- Outlier detection

#### Data Validation
- Schema validation against expected fields
- Range checks for numerical values
- Date format validation
- Null/empty field detection

**Usage Example**:
```python
from tools import dataclean
import pandas as pd

# Read and clean CSV
df = pd.read_csv('data.csv', delimiter=';', encoding='latin1')
# Apply cleaning functions from dataclean module
```

---

### tools/etl_utils.py

**Purpose**: Common utilities for ETL operations

**Typical Functions**:
- Date parsing and normalization
- Coordinate/location formatting
- Unit conversion (temperature, distance, etc.)
- Data validation helpers
- Logging utilities

**Usage**: Shared across multiple ETL modules to reduce duplication

---

## Common Patterns

### ETL Module Structure

All ETL modules follow this pattern:

```python
#!/usr/bin/env python
""" module_name.py

Brief description of the module.
"""

from absl import flags, app, logging
from tools import database as db

FLAGS = flags.FLAGS

def define_flags():
    """Define command-line arguments"""
    flags.DEFINE_string('source_path', None, 'Path to data')

def main(argv):
    """Main ETL execution"""
    logging.info('='*80)
    logging.info(' '*20 + 'ETL Module Name')
    logging.info('='*80)
    
    # Validate arguments
    if FLAGS.source_path is None:
        logging.error('Source path required')
        return 1
    
    # Connect to database
    client = db.connect_mongo_daemon()
    database = db.get_mongo_database(client, 'aire')
    
    # Extract data
    # Transform data
    # Load data
    
    logging.info('ETL completed successfully')

if __name__ == '__main__':
    define_flags()
    app.run(main)
```

### Error Handling Pattern

```python
try:
    # Operation
except FileNotFoundError:
    logging.error(f'File not found: {path}')
    sys.exit(1)
except Exception as e:
    logging.error(f'Unexpected error: {str(e)}')
    sys.exit(2)
```

### Database Insert Pattern

```python
# Single document
doc = {'field': 'value', 'timestamp': datetime.now()}
doc_id = db.insert_one_document(database, 'collection', doc)
logging.info(f'Inserted document: {doc_id}')

# Multiple documents (preferred for batch)
docs = [{'field': f'value{i}'} for i in range(100)]
doc_ids = db.insert_many_documents(database, 'collection', docs)
logging.info(f'Inserted {len(doc_ids)} documents')
```

---

## Code Examples

### Complete ETL Example: Processing Weather CSV

```python
#!/usr/bin/env python
"""Example: Process weather data from CSV"""

import pandas as pd
from datetime import datetime
from tools import database as db

# Connect to database
client = db.connect_mongo_daemon()
database = db.get_mongo_database(client, 'aire')

# Read CSV file
df = pd.read_csv('weather_data.csv', delimiter=';', encoding='latin1')

# Transform to documents
documents = []
for idx, row in df.iterrows():
    doc = {
        'station_id': row['id_estacion'],
        'station_name': row['nombre'],
        'temperature': float(row['temperatura']),
        'humidity': float(row['humedad']),
        'wind_speed': float(row['velocidad_viento']),
        'timestamp': datetime.strptime(row['fecha'], '%Y-%m-%d %H:%M:%S'),
        'location': {
            'latitude': float(row['latitud']),
            'longitude': float(row['longitud'])
        }
    }
    documents.append(doc)

# Insert into database
inserted_ids = db.insert_many_documents(database, 'weather', documents)
print(f"Successfully inserted {len(inserted_ids)} weather documents")
```

### Querying Loaded Data

```python
#!/usr/bin/env python
"""Example: Query weather data from MongoDB"""

from tools import database as db
from datetime import datetime, timedelta

# Connect to database
client = db.connect_mongo_daemon()
database = db.get_mongo_database(client, 'aire')
weather = db.get_mongo_collection(database, 'weather')

# Find recent weather data
cutoff = datetime.now() - timedelta(days=7)
recent_docs = weather.find({
    'timestamp': {'$gte': cutoff},
    'temperature': {'$gte': 20}
})

# Process results
for doc in recent_docs:
    print(f"{doc['station_name']}: {doc['temperature']}°C at {doc['timestamp']}")

# Aggregation example
pipeline = [
    {'$match': {'timestamp': {'$gte': cutoff}}},
    {'$group': {
        '_id': '$station_id',
        'avg_temp': {'$avg': '$temperature'},
        'count': {'$sum': 1}
    }},
    {'$sort': {'avg_temp': -1}}
]

results = weather.aggregate(pipeline)
for result in results:
    print(f"Station {result['_id']}: Avg {result['avg_temp']:.1f}°C ({result['count']} readings)")
```

### Creating Indexes for Performance

```python
#!/usr/bin/env python
"""Example: Create database indexes"""

from tools import database as db

client = db.connect_mongo_daemon()
database = db.get_mongo_database(client, 'aire')

# Create indexes for better query performance
collections = {
    'weather': [('timestamp', 1), ('station_id', 1)],
    'traffic': [('timestamp', 1), ('pmed_id', 1)],
    'air_quality': [('timestamp', 1), ('station_id', 1)]
}

for collection_name, indexes in collections.items():
    coll = database[collection_name]
    for fields in indexes:
        coll.create_index([fields])
        print(f"Created index on {collection_name}.{fields}")
```

---

## Module Dependencies

```
etl_weather.py
  ├─ absl (logging, flags)
  ├─ tools.database
  └─ json, datetime

etl_traffic.py
  ├─ absl (logging)
  ├─ pandas
  ├─ numpy
  └─ tools.database

etl_calendar.py
  ├─ absl (logging, flags, app)
  ├─ pandas
  └─ tools.database

request_weather_data.py
  └─ [API libraries, requests]

tools/database.py
  └─ pymongo

tools/dataclean.py
  ├─ pandas
  ├─ datetime
  └─ glob

tools/etl_utils.py
  ├─ datetime
  └─ numpy
```

---

For more information, see:
- [README.md](README.md) - Project overview
- [ARCHITECTURE.md](ARCHITECTURE.md) - System design
- [SETUP.md](SETUP.md) - Installation guide
