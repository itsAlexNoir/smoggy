# Quick Reference

Quick lookup guide for common Smoggy tasks.

## Getting Started

### 1. Setup
```bash
# Clone, setup virtual environment
git clone <repo>
cd smoggy
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Start MongoDB
./bin/start_smoggydb.sh
```

### 2. Run ETL Pipeline
```bash
# Weather data
python etl/etl_weather.py --source_path=/data/weather

# Traffic data
python etl/etl_traffic.py --source_path=/data/traffic

# Calendar data
python etl/etl_calendar.py --source_path=/data/calendar
```

### 3. Query Data
```bash
# Open Python shell
python3

# In Python:
from tools.database import connect_mongo_daemon, get_mongo_database
client = connect_mongo_daemon()
db = get_mongo_database(client, 'aire')

# Query
weather = db.weather.find_one()
print(weather)
```

### 4. Analyze Data
```bash
# Start Jupyter
jupyter notebook

# Open notebooks/1_analisis_calidad_aire.ipynb
```

## Common Tasks

### Check MongoDB Connection
```bash
python3 -c "from pymongo import MongoClient; print('✓ Connected' if MongoClient().list_database_names() else '✗ Failed')"
```

### Count Documents in Collection
```python
from tools.database import connect_mongo_daemon, get_mongo_database
client = connect_mongo_daemon()
db = get_mongo_database(client, 'aire')
print(f"Weather records: {db.weather.count_documents({})}")
print(f"Traffic records: {db.traffic.count_documents({})}")
```

### Find Recent Data (Last 7 Days)
```python
from datetime import datetime, timedelta
from tools.database import connect_mongo_daemon, get_mongo_database

client = connect_mongo_daemon()
db = get_mongo_database(client, 'aire')

cutoff = datetime.now() - timedelta(days=7)
recent = db.weather.find({'timestamp': {'$gte': cutoff}})
print(f"Recent weather records: {recent.count()}")
```

### Export Data to CSV
```python
import pandas as pd
from tools.database import connect_mongo_daemon, get_mongo_database

client = connect_mongo_daemon()
db = get_mongo_database(client, 'aire')

# Export weather to CSV
df = pd.DataFrame(list(db.weather.find({}, {'_id': 0})))
df.to_csv('weather_export.csv', index=False)
```

### Delete Old Data (Cleanup)
```python
from datetime import datetime, timedelta
from tools.database import connect_mongo_daemon, get_mongo_database

client = connect_mongo_daemon()
db = get_mongo_database(client, 'aire')

# Delete records older than 1 year
cutoff = datetime.now() - timedelta(days=365)
result = db.weather.delete_many({'timestamp': {'$lt': cutoff}})
print(f"Deleted {result.deleted_count} records")
```

### Create Indexes
```python
from tools.database import connect_mongo_daemon, get_mongo_database

client = connect_mongo_daemon()
db = get_mongo_database(client, 'aire')

# Create indexes for faster queries
db.weather.create_index([('timestamp', 1)])
db.weather.create_index([('station_id', 1)])
print("✓ Indexes created")
```

### Backup Database
```bash
# Backup entire database
mongodump --db aire --out backup/

# Restore database
mongorestore backup/aire
```

## File Locations

| Item | Location |
|------|----------|
| Main Code | `etl/`, `tools/` |
| Notebooks | `notebooks/` |
| Startup Scripts | `bin/` |
| License | `LICENSE` |
| API Key | `tools/aemet-api-key` |

## Command Flags

### ETL Modules

```bash
# Weather ETL
python etl/etl_weather.py \
  --source_path=/path/to/weather/data

# Traffic ETL
python etl/etl_traffic.py \
  --source_path=/path/to/traffic/data

# Calendar ETL
python etl/etl_calendar.py \
  --source_path=/path/to/calendar/data
```

## Troubleshooting

### MongoDB Won't Start
```bash
# Check if already running
ps aux | grep mongod

# Kill existing process
pkill mongod

# Try starting again
mongod --dbpath ~/data/db
```

### Can't Import Module
```bash
# Check virtual environment is activated
which python  # Should be in venv/bin/

# Reinstall packages
pip install --upgrade pymongo pandas numpy absl-py
```

### PMED Data Import Issues
- Ensure CSV uses semicolon (`;`) delimiter
- Check encoding is Latin-1
- Verify column names match expected format

### API Key Not Found
```bash
# Set API key
echo "your-api-key" > tools/aemet-api-key
chmod 600 tools/aemet-api-key
```

## Key Classes & Functions

### Database Operations
| Function | Purpose |
|----------|---------|
| `connect_mongo_daemon()` | Connect to MongoDB |
| `get_mongo_database()` | Access database |
| `get_mongo_collection()` | Access collection |
| `insert_one_document()` | Insert single doc |
| `insert_many_documents()` | Insert multiple docs |

### ETL Main Functions
| Module | Function | Purpose |
|--------|----------|---------|
| `etl_weather.py` | `main()` | Load weather data |
| `etl_traffic.py` | `main()` | Load traffic data |
| `etl_calendar.py` | `main()` | Load calendar data |

## Data Schema Quick View

### Weather Document
```json
{
  "station_id": "estacion_001",
  "station_name": "Madrid Centro",
  "temperature": 22.5,
  "humidity": 65,
  "wind_speed": 3.2,
  "timestamp": "2024-01-15T10:30:00Z",
  "location": {"latitude": 40.42, "longitude": -3.70}
}
```

### Traffic Document
```json
{
  "pmed_id": "pmed_001",
  "location": "Calle Gran Via",
  "vehicle_count": 1250,
  "vehicle_density": 45.3,
  "timestamp": "2024-01-15T10:30:00Z",
  "intensity": 78
}
```

### Calendar Document
```json
{
  "date": "2024-01-15",
  "day": 15,
  "month": 1,
  "year": 2024
}
```

## Performance Tips

### Speed Up Bulk Inserts
```python
# Use batch insertion instead of loops
docs = [doc1, doc2, doc3, ...]  # Collect all docs first
db.collection.insert_many(docs)  # One database call
```

### Query Optimization
```python
# Use indexes on frequently queried fields
db.weather.create_index([('timestamp', 1)])
db.weather.create_index([('station_id', 1)])

# Use projection to fetch only needed fields
db.weather.find({}, {'timestamp': 1, 'temperature': 1})
```

### Connection Pooling
```python
# Reuse client connection instead of creating new ones
client = connect_mongo_daemon()  # Once
db = get_mongo_database(client, 'aire')  # Reuse
collection = get_mongo_collection(db, 'weather')  # Reuse
```

## Useful Commands

```bash
# List all databases
mongo --eval "db.adminCommand('listDatabases')"

# Connect to database
mongo aire

# Show collections in database
# (in mongo shell)
show collections

# Count documents
# (in mongo shell)
db.weather.count()

# Find document by ID
# (in mongo shell)
db.weather.findOne({_id: ObjectId("...")})
```

## Documentation Index

| Document | Purpose |
|----------|---------|
| [README.md](README.md) | Project overview |
| [SETUP.md](SETUP.md) | Installation guide |
| [ARCHITECTURE.md](ARCHITECTURE.md) | System design |
| [MODULES.md](MODULES.md) | Module documentation |
| [CONTRIBUTING.md](CONTRIBUTING.md) | Contribution guide |
| [QUICK_REFERENCE.md](QUICK_REFERENCE.md) | This file |

## Support

- Check [SETUP.md - Troubleshooting](SETUP.md#troubleshooting)
- Review [MODULES.md](MODULES.md) for function details
- See [CONTRIBUTING.md](CONTRIBUTING.md) for development help
