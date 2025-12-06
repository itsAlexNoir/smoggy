# Smoggy Documentation Index

Welcome to the Smoggy project documentation. This index will help you navigate all available resources.

## 📚 Documentation Overview

| Document | Purpose | Best For |
|----------|---------|----------|
| **[README.md](README.md)** | Project overview and introduction | Getting started, understanding the project |
| **[SETUP.md](SETUP.md)** | Installation and configuration guide | Setting up development environment |
| **[ARCHITECTURE.md](ARCHITECTURE.md)** | System design and components | Understanding the technical design |
| **[MODULES.md](MODULES.md)** | Detailed module documentation | Understanding code structure and APIs |
| **[QUICK_REFERENCE.md](QUICK_REFERENCE.md)** | Quick lookup for common tasks | Fast reference while coding |
| **[CONTRIBUTING.md](CONTRIBUTING.md)** | Contribution guidelines | Contributing to the project |
| **[CHANGELOG.md](CHANGELOG.md)** | Version history and changes | Tracking project updates |

## 🚀 Quick Start

### For First-Time Users

1. **Start here**: [README.md](README.md) - Understand what Smoggy does
2. **Then read**: [SETUP.md](SETUP.md) - Install and configure
3. **Explore**: [MODULES.md](MODULES.md) - Learn about the code
4. **Try it out**: Run the example in [QUICK_REFERENCE.md](QUICK_REFERENCE.md)

### Installation (2 minutes)

```bash
# 1. Clone repository
git clone <repository-url>
cd smoggy

# 2. Setup Python environment
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# 3. Start MongoDB
./bin/start_smoggydb.sh

# 4. Run your first ETL job
python etl/etl_calendar.py --source_path=/path/to/data
```

See [SETUP.md](SETUP.md) for detailed instructions.

## 📖 Documentation Detailed Guide

### README.md
**Read if you want to:**
- Understand the project purpose
- See the project structure
- Learn what components exist
- Get high-level overview
- Run the ETL pipeline
- Analyze data with notebooks

**Key sections:**
- Overview of the project
- Complete project structure
- Component descriptions
- Getting started instructions
- Data sources
- Database schema overview
- Author and license information

### SETUP.md
**Read if you need to:**
- Install the project
- Configure MongoDB
- Set up API keys
- Debug installation issues
- Verify your setup

**Key sections:**
- Prerequisites checklist
- Step-by-step installation
- MongoDB setup and configuration
- Environment configuration
- Installation verification
- Comprehensive troubleshooting guide

### ARCHITECTURE.md
**Read if you want to:**
- Understand system design
- See data flow diagrams
- Learn about technology stack
- Understand deployment architecture
- Learn about performance considerations
- Extend the system

**Key sections:**
- System overview diagram
- Data flow examples
- Database schema definitions
- Component descriptions
- Technology stack details
- Deployment architecture
- Extension points for customization

### MODULES.md
**Read if you need to:**
- Understand code structure
- Learn about ETL modules
- Use utility functions
- See code examples
- Understand common patterns
- Query the database

**Key sections:**
- ETL module documentation
- Tools module documentation
- Common coding patterns
- Practical code examples
- Module dependencies
- Function signatures

### QUICK_REFERENCE.md
**Read if you want to:**
- Quickly look up a command
- Find code snippets
- Remember file locations
- Troubleshoot common issues
- Optimize performance
- Refresh your memory

**Key sections:**
- Getting started checklist
- Common tasks with code
- File locations table
- Troubleshooting quick fixes
- Key functions reference
- Performance tips
- Useful command reference

### CONTRIBUTING.md
**Read if you want to:**
- Contribute to the project
- Understand coding standards
- Know how to submit changes
- Learn development guidelines
- Understand the review process

**Key sections:**
- Code of conduct
- How to contribute
- Coding standards
- Documentation standards
- Testing requirements
- Pull request process
- Areas for contribution

### CHANGELOG.md
**Read if you want to:**
- See what changed in versions
- Track project history
- Understand version numbering
- Know the release process
- Plan future features

**Key sections:**
- Unreleased changes
- Version history
- Release process
- Versioning scheme
- Format guidelines

## 🎯 Learning Paths

### Path 1: User (Just Want to Use the Project)
1. **README.md** - Understand what it does
2. **SETUP.md** - Get it running
3. **QUICK_REFERENCE.md** - Learn common tasks
4. **Notebooks** - Analyze data

### Path 2: Developer (Want to Modify Code)
1. **README.md** - Project overview
2. **SETUP.md** - Development setup
3. **ARCHITECTURE.md** - Understand design
4. **MODULES.md** - Learn code structure
5. **CONTRIBUTING.md** - Contribution guidelines

### Path 3: DevOps/Deployment (Want to Deploy)
1. **README.md** - Overview
2. **SETUP.md** - Installation details
3. **ARCHITECTURE.md** - Deployment considerations
4. **QUICK_REFERENCE.md** - Common commands

### Path 4: Maintainer (Ongoing Management)
1. All documentation for comprehensive understanding
2. **CHANGELOG.md** - Track changes
3. **CONTRIBUTING.md** - Review contributions

## 📋 Common Questions & Where to Find Answers

| Question | Document | Section |
|----------|----------|---------|
| What does Smoggy do? | README.md | Overview |
| How do I install it? | SETUP.md | Installation |
| How do I run the ETL? | README.md | Running the ETL Pipeline |
| What's the database schema? | ARCHITECTURE.md | Database Schema |
| How do I query data? | MODULES.md | Code Examples |
| What's the project structure? | README.md | Project Structure |
| How do I troubleshoot? | SETUP.md | Troubleshooting |
| How do I contribute? | CONTRIBUTING.md | How to Contribute |
| What changed in v0.2? | CHANGELOG.md | Version History |
| What modules exist? | MODULES.md | Module Documentation |
| How do I optimize performance? | QUICK_REFERENCE.md | Performance Tips |

## 🔧 Useful Commands

```bash
# Get all documentation
ls -la *.md

# Search documentation
grep -r "keyword" *.md

# Count lines in documentation
wc -l *.md

# View specific section
grep -A 20 "## Section" README.md

# Quick documentation index
head -50 DOCUMENTATION_INDEX.md
```

## 📞 Getting Help

### Before Asking

1. **Check the relevant documentation** listed above
2. **Search QUICK_REFERENCE.md** for common issues
3. **Review SETUP.md troubleshooting** for setup issues
4. **Look in MODULES.md examples** for code help

### Where to Get Help

- **Installation Issues**: See [SETUP.md - Troubleshooting](SETUP.md#troubleshooting)
- **Code Questions**: See [MODULES.md](MODULES.md)
- **Design Questions**: See [ARCHITECTURE.md](ARCHITECTURE.md)
- **How-To Questions**: See [QUICK_REFERENCE.md](QUICK_REFERENCE.md)
- **Contributing**: See [CONTRIBUTING.md](CONTRIBUTING.md)

### Contact

- **Email**: alejandrodelacallenegro@gmail.com
- **Issues**: Create an issue in the repository
- **Discussion**: Use repository discussions

## 📊 Documentation Statistics

| File | Type | Size | Lines | Purpose |
|------|------|------|-------|---------|
| README.md | Overview | 6.4K | ~190 | Project introduction |
| SETUP.md | Guide | 9.3K | ~280 | Installation instructions |
| ARCHITECTURE.md | Design | 15K | ~450 | System architecture |
| MODULES.md | Reference | 13K | ~390 | Code documentation |
| QUICK_REFERENCE.md | Quick Guide | 6.8K | ~210 | Common tasks |
| CONTRIBUTING.md | Guidelines | 3.7K | ~110 | Contribution rules |
| CHANGELOG.md | History | 3.5K | ~100 | Version tracking |
| **Total** | | **~57K** | **~1,730** | Complete documentation |

## 📝 Documentation Format

All documentation uses:
- **Markdown** format for readability
- **Section headers** for easy navigation
- **Code blocks** with syntax highlighting
- **Tables** for structured information
- **Links** to related sections
- **Examples** for practical understanding

## 🔄 Documentation Maintenance

The documentation is maintained alongside the code. When making changes:

1. **Update relevant documentation files**
2. **Keep examples current**
3. **Update CHANGELOG.md**
4. **Verify links work**
5. **Maintain consistent formatting**

See [CONTRIBUTING.md](CONTRIBUTING.md) for contribution guidelines.

## 🎓 Further Learning

### Related Topics

- [MongoDB Documentation](https://docs.mongodb.com/)
- [PyMongo Guide](https://pymongo.readthedocs.io/)
- [Pandas Tutorial](https://pandas.pydata.org/docs/)
- [Jupyter Documentation](https://jupyter.org/)
- [Python Best Practices](https://docs.python-guide.org/)

### Data Sources

- [Madrid Open Data Portal](https://datos.madrid.es/)
- [AEMET - Spanish Weather Service](https://www.aemet.es/)

---

**Last Updated**: December 6, 2024

For the most current documentation, always check the repository directly.

## Navigation

🔗 **Quick Links**
- [Go to README](README.md)
- [Go to SETUP](SETUP.md)
- [Go to ARCHITECTURE](ARCHITECTURE.md)
- [Go to MODULES](MODULES.md)
- [Go to QUICK REFERENCE](QUICK_REFERENCE.md)
- [Go to CONTRIBUTING](CONTRIBUTING.md)
- [Go to CHANGELOG](CHANGELOG.md)
