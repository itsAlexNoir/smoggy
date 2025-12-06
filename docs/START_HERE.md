# 🚀 START HERE

Welcome to the Smoggy documentation! This guide will help you get oriented quickly.

## What is Smoggy?

Smoggy is an ETL (Extract, Transform, Load) pipeline that aggregates air quality, weather, traffic, and calendar data from Madrid's open data sources. It uses MongoDB to store unified data for analysis and machine learning predictions.

## Choose Your Path

### 👤 I'm a **First-Time User**
→ Start here: [README.md](README.md)
- Understand what the project does
- See the complete structure
- Learn about components
- Get general overview (5 min read)

### 🔧 I Want to **Install & Run It**
→ Go to: [SETUP.md](SETUP.md)
- Step-by-step installation
- MongoDB configuration
- Troubleshooting help
- Verification procedures (15 min)

### 👨‍💻 I'm a **Developer**
→ Then read: [ARCHITECTURE.md](ARCHITECTURE.md) + [MODULES.md](MODULES.md)
- System design and architecture
- Database schemas
- Code structure and modules
- Code examples and patterns (30 min)

### ⚡ I Need **Quick Reference**
→ Use: [QUICK_REFERENCE.md](QUICK_REFERENCE.md)
- Common commands and tasks
- Copy-paste code snippets
- File locations
- Troubleshooting quick fixes (ongoing reference)

### 🤝 I Want to **Contribute**
→ Read: [CONTRIBUTING.md](CONTRIBUTING.md)
- Code standards
- How to contribute
- Pull request process
- Development guidelines

### 📚 I Want **Full Navigation**
→ Use: [DOCUMENTATION_INDEX.md](DOCUMENTATION_INDEX.md)
- Complete documentation map
- Multiple learning paths
- Search by topic
- Find anything quickly

---

## Quick Start (3 minutes)

```bash
# 1. Clone and setup
git clone <repository-url>
cd smoggy
python3 -m venv venv
source venv/bin/activate
pip install pymongo pandas numpy absl-py

# 2. Start MongoDB
./bin/start_smoggydb.sh

# 3. Run your first ETL
python etl/etl_calendar.py --source_path=/path/to/data

# 4. Query the data
python3 -c "
from tools.database import connect_mongo_daemon
client = connect_mongo_daemon()
db = client['aire']
print(f'✓ Database has {db.list_collection_names()} collections')
"
```

Done! 🎉

---

## Documentation Files at a Glance

| File | Size | Purpose | Time |
|------|------|---------|------|
| [README.md](README.md) | 6KB | Project overview | 5 min |
| [SETUP.md](SETUP.md) | 9KB | Installation guide | 15 min |
| [ARCHITECTURE.md](ARCHITECTURE.md) | 15KB | System design | 10 min |
| [MODULES.md](MODULES.md) | 13KB | Code documentation | 20 min |
| [QUICK_REFERENCE.md](QUICK_REFERENCE.md) | 7KB | Fast lookup | Ongoing |
| [CONTRIBUTING.md](CONTRIBUTING.md) | 4KB | Contribution rules | 5 min |
| [CHANGELOG.md](CHANGELOG.md) | 4KB | Version history | As needed |
| [DOCUMENTATION_INDEX.md](DOCUMENTATION_INDEX.md) | 9KB | Navigation hub | 5 min |

**Total**: 2,461 lines of documentation across 8 files

---

## Common Questions

**Q: How do I install Smoggy?**
A: See [SETUP.md](SETUP.md) for detailed instructions.

**Q: What does the ETL pipeline do?**
A: See [README.md](README.md#overview) for an overview, then [ARCHITECTURE.md](ARCHITECTURE.md) for technical details.

**Q: How do I run the code?**
A: See [QUICK_REFERENCE.md](QUICK_REFERENCE.md#getting-started) for quick commands.

**Q: How do I troubleshoot issues?**
A: See [SETUP.md#troubleshooting](SETUP.md#troubleshooting) for common solutions.

**Q: How can I contribute?**
A: See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

**Q: What's the database structure?**
A: See [ARCHITECTURE.md#database-schema](ARCHITECTURE.md#database-schema) for schemas.

**Q: How do I query the data?**
A: See [MODULES.md#code-examples](MODULES.md#code-examples) for query examples.

---

## Project Structure

```
smoggy/
├── 📖 Documentation (YOU ARE HERE)
│   ├── START_HERE.md ..................... This file
│   ├── README.md ........................ Project overview
│   ├── SETUP.md ......................... Installation guide
│   ├── ARCHITECTURE.md .................. System design
│   ├── MODULES.md ....................... Code documentation
│   ├── QUICK_REFERENCE.md ............... Quick lookup
│   ├── CONTRIBUTING.md .................. Contribution guide
│   ├── CHANGELOG.md ..................... Version history
│   └── DOCUMENTATION_INDEX.md ........... Navigation hub
│
├── 🔧 Core Application
│   ├── etl/ ............................. ETL pipeline
│   │   ├── etl_weather.py
│   │   ├── etl_traffic.py
│   │   ├── etl_calendar.py
│   │   └── request_weather_data.py
│   │
│   └── tools/ ........................... Utilities
│       ├── database.py
│       ├── dataclean.py
│       └── etl_utils.py
│
├── 📊 Data Analysis
│   └── notebooks/ ....................... Jupyter notebooks
│       ├── 1_analisis_calidad_aire.ipynb
│       ├── get_pollution_station_info.ipynb
│       └── pollution_viewer.ipynb
│
├── 🚀 Deployment
│   └── bin/ ............................. Scripts
│       ├── start_smoggydb.sh
│       └── start_tiny_smoggydb.sh
│
└── 📋 Config
    ├── LICENSE
    ├── README.md
    └── .gitignore
```

---

## Next Steps

Choose what you want to do:

### 🎓 Learn
1. [README.md](README.md) - Understand the project
2. [ARCHITECTURE.md](ARCHITECTURE.md) - Learn the design
3. Explore the notebooks in `notebooks/`

### 💻 Develop
1. [SETUP.md](SETUP.md) - Install locally
2. [MODULES.md](MODULES.md) - Understand the code
3. [CONTRIBUTING.md](CONTRIBUTING.md) - Make changes

### 🔍 Reference
1. Bookmark [QUICK_REFERENCE.md](QUICK_REFERENCE.md)
2. Use [DOCUMENTATION_INDEX.md](DOCUMENTATION_INDEX.md) for navigation
3. Check [MODULES.md](MODULES.md) for API documentation

### 🐛 Troubleshoot
1. [SETUP.md#troubleshooting](SETUP.md#troubleshooting) - Common solutions
2. [QUICK_REFERENCE.md#troubleshooting](QUICK_REFERENCE.md#troubleshooting) - Quick fixes

---

## Resources

- **Project Repository**: Check the .git directory
- **Author**: Alejandro de la Calle (alejandrodelacallenegro@gmail.com)
- **License**: GNU General Public License v3 (see [LICENSE](../LICENSE))
- **Python Version**: 3.7+
- **Database**: MongoDB 4.0+

---

## Help & Support

1. **Check the documentation** - Most answers are there
2. **Search [QUICK_REFERENCE.md](QUICK_REFERENCE.md)** - For common tasks
3. **Review [SETUP.md#troubleshooting](SETUP.md#troubleshooting)** - For installation issues
4. **Create an issue** - On the repository

---

## Fun Facts

- 📈 Over 2,400 lines of documentation
- 🌡️ Tracks weather, traffic, air quality & calendar
- 📦 MongoDB-based data warehouse
- 🐍 100% Python implementation
- 🚀 Ready to extend with ML models

---

## Final Note

You're now ready to explore Smoggy! Pick a path above and start your journey.

**Recommended first step**: Read [README.md](README.md) (5 minutes) 👈

Good luck! 🎯
