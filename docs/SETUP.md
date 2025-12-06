# Setup Guide

Comprehensive guide for setting up the Smoggy project for development and usage.

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Installation](#installation)
3. [MongoDB Setup](#mongodb-setup)
4. [Configuration](#configuration)
5. [Verification](#verification)
6. [Troubleshooting](#troubleshooting)

## Prerequisites

### System Requirements

- **Operating System**: Linux, macOS, or Windows (with WSL)
- **Python**: 3.7 or higher
- **MongoDB**: 4.0 or higher
- **Disk Space**: Minimum 10GB for database (depending on data volume)
- **RAM**: Minimum 4GB recommended for processing large datasets

### Software Installation

#### macOS

Using Homebrew:

```bash
# Install Python 3
brew install python3

# Install MongoDB
brew install mongodb-community

# Install Jupyter (optional, for notebooks)
brew install jupyter
```

#### Ubuntu/Debian

```bash
# Update package list
sudo apt-get update

# Install Python 3 and pip
sudo apt-get install python3 python3-pip

# Install MongoDB
curl https://www.mongodb.org/static/pgp/server-6.0.asc | sudo apt-key add -
echo "deb [ arch=amd64,arm64 ] https://repo.mongodb.org/apt/ubuntu focal/mongodb-org/6.0 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-6.0.list
sudo apt-get update
sudo apt-get install -y mongodb-org
```

#### Windows

1. Install Python 3 from [python.org](https://www.python.org/downloads/)
2. Install MongoDB Community Edition from [mongodb.com](https://www.mongodb.com/try/download/community)
3. Install Git from [git-scm.com](https://git-scm.com/download/win)

## Installation

### 1. Clone the Repository

```bash
git clone <repository-url>
cd smoggy
```

### 2. Create Python Virtual Environment

```bash
# Create virtual environment
python3 -m venv venv

# Activate virtual environment
# On macOS/Linux:
source venv/bin/activate

# On Windows:
venv\Scripts\activate
```

### 3. Install Python Dependencies

```bash
# Upgrade pip
pip install --upgrade pip

# Install project dependencies
pip install -r requirements.txt
```

If `requirements.txt` doesn't exist, install packages manually:

```bash
pip install pymongo pandas numpy absl-py jupyter matplotlib seaborn scikit-learn
```

### 4. Set Up MongoDB

#### macOS

```bash
# Create data directory
mkdir -p ~/data/db

# Start MongoDB daemon
mongod --dbpath ~/data/db
```

Or use the provided startup script:

```bash
# Ensure MongoDB is installed and accessible
./bin/start_smoggydb.sh
```

#### Ubuntu/Debian

```bash
# Create data directory
sudo mkdir -p /data/db
sudo chown $(whoami) /data/db

# Start MongoDB service
sudo systemctl start mongod

# Enable auto-start on boot
sudo systemctl enable mongod
```

#### Verify MongoDB Connection

```bash
# Test connection
python3 -c "from pymongo import MongoClient; client = MongoClient(); print('Connected' if client.list_database_names() else 'Failed')"
```

## MongoDB Setup

### Create Database and Collections

```bash
# Connect to MongoDB
mongo

# Create database
use aire

# Create collections
db.createCollection("weather")
db.createCollection("traffic")
db.createCollection("air_quality")
db.createCollection("calendar")

# Create indexes for performance
db.weather.createIndex({ "timestamp": 1 })
db.weather.createIndex({ "station_id": 1 })
db.traffic.createIndex({ "timestamp": 1 })
db.traffic.createIndex({ "pmed_id": 1 })
db.air_quality.createIndex({ "timestamp": 1 })
db.air_quality.createIndex({ "station_id": 1 })
db.calendar.createIndex({ "date": 1 })

# Exit MongoDB shell
exit
```

### Alternative: Using Python

```python
from pymongo import MongoClient

client = MongoClient()
db = client['aire']

# Create collections
db.create_collection('weather')
db.create_collection('traffic')
db.create_collection('air_quality')
db.create_collection('calendar')

# Create indexes
db.weather.create_index([('timestamp', 1)])
db.weather.create_index([('station_id', 1)])
db.traffic.create_index([('timestamp', 1)])
db.traffic.create_index([('pmed_id', 1)])
db.air_quality.create_index([('timestamp', 1)])
db.air_quality.create_index([('station_id', 1)])
db.calendar.create_index([('date', 1)])

print("Database and collections created successfully")
```

## Configuration

### MongoDB Connection

The default connection uses localhost:27017. To customize:

Edit `tools/database.py`:

```python
def connect_mongo_daemon(host='localhost', port=27017):
    """Connect to MongoDB with custom host/port"""
    client = MongoClient(host, port)
    return client
```

Or set environment variables:

```bash
export MONGO_HOST=localhost
export MONGO_PORT=27017
```

### API Keys

#### AEMET API Key

1. Register at [AEMET API](https://www.aemet.es/es/datos_abiertos_aemet)
2. Save your API key to `tools/aemet-api-key`:

```bash
echo "your-api-key-here" > tools/aemet-api-key
```

Protect the file:

```bash
chmod 600 tools/aemet-api-key
```

### Data Source Configuration

Prepare your data directories with the following structure:

```
data/
├── weather/
│   └── estaciones_meteo.json
├── traffic/
│   ├── pmed_*.csv
│   └── density_*.csv
└── calendar/
    └── calendario.csv
```

## Verification

### Test Installation

```bash
# Test Python packages
python3 -c "import pymongo, pandas, numpy; print('✓ All packages installed')"

# Test MongoDB connection
python3 -c "from tools.database import connect_mongo_daemon; client = connect_mongo_daemon(); print('✓ MongoDB connected')"

# Test ETL modules import
python3 -c "from etl import etl_weather, etl_traffic, etl_calendar; print('✓ All ETL modules available')"
```

### Run Sample ETL

Test the calendar ETL with sample data:

```bash
# Create sample calendar data
mkdir -p data/calendar
cat > data/calendar/calendario.csv << 'EOF'
Dia
01/01/2024
02/01/2024
03/01/2024
EOF

# Run calendar ETL
python3 etl/etl_calendar.py --source_path=data/calendar

# Verify data in database
python3 << 'EOF'
from tools.database import connect_mongo_daemon, get_mongo_database, get_mongo_collection
client = connect_mongo_daemon()
db = get_mongo_database(client, 'aire')
calendar = get_mongo_collection(db, 'calendar')
print(f"✓ {calendar.count_documents({})} calendar entries loaded")
EOF
```

### Launch Jupyter

```bash
# Start Jupyter notebook server
jupyter notebook

# Open browser and navigate to http://localhost:8888
# Open notebook in notebooks/ directory
```

## Troubleshooting

### MongoDB Connection Failed

**Error**: `ConnectionFailure: Error connecting to localhost:27017`

**Solution**:
1. Verify MongoDB is running:
   ```bash
   # macOS
   brew services list | grep mongodb
   
   # Linux
   sudo systemctl status mongod
   ```

2. Check port is accessible:
   ```bash
   netstat -an | grep 27017
   ```

3. Restart MongoDB:
   ```bash
   # macOS
   brew services restart mongodb-community
   
   # Linux
   sudo systemctl restart mongod
   ```

### Python Package Import Error

**Error**: `ModuleNotFoundError: No module named 'pymongo'`

**Solution**:
```bash
# Ensure virtual environment is activated
source venv/bin/activate

# Reinstall packages
pip install --upgrade pymongo pandas numpy absl-py
```

### Permission Denied on Startup Scripts

**Error**: `Permission denied: './bin/start_smoggydb.sh'`

**Solution**:
```bash
chmod +x bin/start_smoggydb.sh
chmod +x bin/start_tiny_smoggydb.sh
./bin/start_smoggydb.sh
```

### ETL Script Can't Find Source Files

**Error**: `FileNotFoundError: estaciones_meteo.json not found`

**Solution**:
1. Verify data directory exists:
   ```bash
   ls -la data/weather/
   ```

2. Provide correct path:
   ```bash
   python3 etl/etl_weather.py --source_path=/path/to/weather/data
   ```

### MongoDB Disk Space Issues

**Error**: `Assertion: unspecified error when writing to /data/db`

**Solution**:
```bash
# Check available space
df -h /data/db

# Archive old data
mongodump --db aire --out backup/

# Remove old documents
python3 << 'EOF'
from datetime import datetime, timedelta
from tools.database import connect_mongo_daemon, get_mongo_database

client = connect_mongo_daemon()
db = get_mongo_database(client, 'aire')

# Remove data older than 1 year
cutoff_date = datetime.now() - timedelta(days=365)
collections = ['weather', 'traffic', 'air_quality']

for collection_name in collections:
    collection = db[collection_name]
    result = collection.delete_many({'timestamp': {'$lt': cutoff_date}})
    print(f"Deleted {result.deleted_count} documents from {collection_name}")
EOF
```

### Jupyter Kernel Issues

**Error**: `Kernel crashed` or `Module not found in notebook`

**Solution**:
```bash
# Install Jupyter kernel for virtual environment
pip install ipykernel
python -m ipykernel install --user --name smoggy --display-name "Smoggy (Python 3)"

# Restart Jupyter and select "Smoggy (Python 3)" kernel
```

## Next Steps

1. [Read the main README.md](README.md) for project overview
2. [Review ARCHITECTURE.md](ARCHITECTURE.md) for system design
3. [Explore the Jupyter notebooks](notebooks/) for data analysis examples
4. [Run the ETL pipeline](README.md#running-the-etl-pipeline) with your data
5. [Check CONTRIBUTING.md](CONTRIBUTING.md) to contribute improvements

## Additional Resources

- [MongoDB Documentation](https://docs.mongodb.com/)
- [PyMongo Guide](https://pymongo.readthedocs.io/)
- [Pandas Documentation](https://pandas.pydata.org/docs/)
- [Jupyter Documentation](https://jupyter.org/documentation)
- [Python Virtual Environments](https://docs.python.org/3/tutorial/venv.html)
