# Contributing to MDA OSINT Database

Thank you for your interest in contributing to the Maritime Domain Awareness OSINT Database.

## Developer Certificate of Origin

By contributing to this project, you certify that your contribution was created by you and that you have the right to submit it under the Apache 2.0 license. All commits must be signed off:

```bash
git commit --signoff -m "Your commit message"
```

## Getting Started

1. Fork the repository
2. Clone your fork: `git clone https://github.com/YOUR_USERNAME/osint_tool.git`
3. Create a feature branch: `git checkout -b feature/your-feature`
4. Make your changes
5. Run tests: `make test`
6. Run linter: `make lint`
7. Commit with sign-off: `git commit --signoff -m "Add feature"`
8. Push and open a Pull Request

## Development Setup

```bash
# Install Python dependencies
pip install -e ".[dev]"

# Start infrastructure
make up

# Wait for services, create topics, init schemas
make setup

# Run tests
make test
```

## Code Style

- Python: Follow PEP 8, enforced via `ruff`
- Line length: 120 characters max
- Type hints: Required for public function signatures
- Docstrings: Required for modules and public classes/functions

## What to Contribute

### High-Impact Areas

- **New data source ingesters** — additional open AIS feeds, sanctions lists, or event databases
- **Anomaly detection rules** — new patterns for AIS manipulation, sanctions evasion, UAS behavior
- **Entity resolution improvements** — better blocking rules, comparison functions, or training data
- **Graph analytics queries** — new Cypher queries for operational intelligence
- **Dashboard templates** — Grafana dashboards for specific operational scenarios
- **Documentation** — deployment guides, data source documentation, API usage examples

### Data Contributions

Community intelligence data can be submitted via the submission validator. See `community/submission_validator.py` for the schema. All submissions must:

1. Exclude PII of private individuals
2. Include at least one source URL
3. Specify a compatible license (CC-BY-4.0 recommended)
4. Pass the automated PII scanner

### Bug Reports

File GitHub issues with:
- Steps to reproduce
- Expected vs actual behavior
- Environment details (OS, Docker version, Python version)
- Relevant log output

## Pull Request Process

1. Update documentation if your change affects the API or configuration
2. Add tests for new functionality
3. Ensure all tests pass: `make test`
4. Ensure linter passes: `make lint`
5. Update CHANGELOG.md if applicable
6. PRs require at least one review before merge

## Architecture Decisions

Major architectural changes should be discussed in a GitHub issue first. This includes:
- New service dependencies
- Schema changes to Memgraph or PostGIS
- New Kafka topics
- Changes to the entity resolution strategy
- New external data source integrations

## License

By contributing, you agree that your contributions will be licensed under the Apache License 2.0.
