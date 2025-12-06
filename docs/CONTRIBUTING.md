# Contributing to Smoggy

Thank you for your interest in contributing to Smoggy! This document provides guidelines and instructions for contributing to the project.

## Code of Conduct

- Be respectful and inclusive
- Provide constructive feedback
- Focus on the code, not the person
- Help maintain a positive community

## How to Contribute

### Reporting Issues

If you find a bug or have a feature request:

1. Check existing issues to avoid duplicates
2. Create a new issue with a clear title and description
3. Include:
   - Steps to reproduce (for bugs)
   - Expected behavior
   - Actual behavior
   - Python version and environment details

### Making Changes

1. **Fork the repository** and create your branch:
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Make your changes** following the coding standards below

3. **Test your changes** thoroughly:
   - Run existing tests
   - Add new tests for new functionality
   - Verify all ETL pipelines work as expected

4. **Commit with clear messages**:
   ```bash
   git commit -m "Add descriptive commit message"
   ```

5. **Push to your fork** and submit a Pull Request

## Coding Standards

### Python Style

- Follow PEP 8 guidelines
- Use meaningful variable and function names
- Add docstrings to all functions and modules
- Keep functions focused and single-purpose

### Module Headers

Include the following header in new Python files:

```python
#!/usr/bin/env python
""" module_name.py

Brief description of what this module does.
"""

__author__ = "Your Name"
__copyright__ = "Copyright 2025"
__license__ = "GPL-3.0"
__version__ = "0.1"
__maintainer__ = "Your Name"
__email__ = "your.email@example.com"
__status__ = "Development"
```

### Documentation

- Add docstrings to all classes and functions
- Update README.md if adding new features
- Include examples for complex functionality
- Document any external dependencies

## ETL Development Guidelines

When working on ETL modules:

1. **Data Validation**: Always validate input data before processing
2. **Error Handling**: Implement proper error handling with logging
3. **Database Operations**: Use the provided database utilities in `tools/database.py`
4. **Data Cleaning**: Use utilities from `tools/dataclean.py` where applicable
5. **Testing**: Test with both full and subset data (see `bin/start_tiny_smoggydb.sh`)

## Testing

- Write tests for new functionality
- Test ETL pipelines with sample data
- Verify MongoDB operations work correctly
- Check data integrity after transformations

## Documentation Standards

- Use Markdown for all documentation
- Include code examples where relevant
- Keep documentation up-to-date with code changes
- Comment complex logic clearly

## Pull Request Process

1. Update documentation and README as needed
2. Ensure all tests pass
3. Provide a clear description of changes
4. Reference any related issues
5. Be responsive to review comments

## Areas for Contribution

### High Priority

- Additional data source integrations
- Performance optimizations for large datasets
- Data quality improvements
- Test coverage expansion

### Medium Priority

- Documentation improvements
- Code refactoring for maintainability
- Additional analysis notebooks
- Configuration management

### Nice to Have

- CI/CD pipeline setup
- Docker containerization
- API layer for data access
- Visualization improvements

## Questions?

If you have questions about contributing:

- Check existing issues and pull requests
- Review the project documentation
- Contact the maintainer at alejandrodelacallenegro@gmail.com

## License

By contributing to Smoggy, you agree that your contributions will be licensed under the GPL-3.0 license.

Thank you for contributing to Smoggy!
