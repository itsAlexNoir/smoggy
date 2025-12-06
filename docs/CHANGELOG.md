# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Comprehensive documentation suite (README, ARCHITECTURE, SETUP, MODULES, CONTRIBUTING)
- Quick reference guide for common tasks
- CHANGELOG template

### Changed
- Updated README with detailed project structure and components
- Expanded documentation for ETL pipeline

### Fixed
- Documentation formatting and clarity

### Deprecated
- (None)

### Removed
- (None)

### Security
- (None)

---

## [0.1] - 2019-01-01

### Added
- Initial ETL pipeline for weather data (`etl_weather.py`)
- Initial ETL pipeline for traffic data (`etl_traffic.py`)
- Initial ETL pipeline for calendar data (`etl_calendar.py`)
- MongoDB database utilities (`tools/database.py`)
- Data cleaning utilities (`tools/dataclean.py`)
- Jupyter notebooks for exploratory data analysis
- Startup scripts for MongoDB daemon

### Initial Features
- Extract weather data from meteorological stations
- Extract traffic data from PMED (traffic measurement devices)
- Extract calendar and temporal information
- Load data into MongoDB collections
- Support for Madrid City Hall open data sources

---

## Format Guidelines

### Added
For new features.

### Changed
For changes in existing functionality.

### Deprecated
For soon-to-be removed features.

### Removed
For now removed features.

### Fixed
For any bug fixes.

### Security
In case of vulnerabilities.

---

## Version History Reference

### 0.1 (Initial Release)
- Core ETL pipeline functionality
- MongoDB integration
- Basic data cleaning utilities

### Future Versions

#### 0.2 (Planned)
- Real-time data streaming support
- Enhanced error handling and recovery
- Performance optimizations
- Additional data source integrations

#### 0.3 (Planned)
- Machine learning model integration
- API layer for data access
- REST endpoints for queries
- Data validation framework

#### 1.0 (Target)
- Production-ready system
- Full test coverage
- Performance optimizations
- Complete documentation
- CI/CD pipeline
- Docker containerization

---

## How to Contribute

When making changes, please:

1. Update this CHANGELOG.md file
2. Follow the format guidelines above
3. Include descriptive messages
4. Reference related issues or PRs
5. Update version numbers if applicable

### Commit Message Format

```
[version] Category: Brief description

Detailed explanation of changes (optional)

Related issues: #123
```

Example:
```
[0.2] Added: Real-time data streaming support

Implemented WebSocket connection handling for streaming
weather data from AEMET API in real-time.

Related issues: #45, #67
```

---

## Release Process

1. Update version number in relevant files
2. Update CHANGELOG.md with all changes
3. Create a git tag: `git tag v0.2.0`
4. Push to repository: `git push origin v0.2.0`
5. Create release on GitHub with changelog excerpt

---

## Versioning Scheme

Smoggy follows [Semantic Versioning](https://semver.org/):

- **MAJOR** version: Incompatible API changes
- **MINOR** version: New functionality in backward compatible manner
- **PATCH** version: Backward compatible bug fixes

Format: `MAJOR.MINOR.PATCH`

Example: `0.1.0`, `1.2.3`

---

## Support & Questions

- For bugs: Create an issue in the repository
- For features: Propose in discussions or issues
- For questions: Check documentation first, then contact maintainer

Email: alejandrodelacallenegro@gmail.com
