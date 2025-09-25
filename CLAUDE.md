# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

### Core Operations
- **Initialize database**: `python main.py init --db-path stock_data.duckdb`
- **Daily data update**: `python main.py daily --tables price_data,financial`
- **Update stock list**: `python main.py update-stock-list`
- **Query database**: `python main.py query --sql "SELECT * FROM stock_list LIMIT 10"`
- **Database info**: `python main.py info`

### Data Import/Export
- **Import positions**: `python main.py import-positions --directory data/account`
- **Import trades**: `python main.py import-trades --file data/trades.csv`
- **Daily account data import**: `python main.py import-account-data-daily --dates 20250903`

### API Server
- **Start API server**: `python api_server/server.py --host 0.0.0.0 --port 5000`
- **Alternative start**: `python api_server/start.py`

### Testing
- **Run all tests**: `python scripts/run_tests.py`
- **JQData tests**: `python run_jqdata_tests.py`

## Architecture Overview

### Core Components

**Database Layer** (`database.py`, `duckdb_impl.py`):
- Abstract database interface with DuckDB implementation
- High-performance columnar storage for financial data
- Handles stock lists, price data, financial statements, and user transactions

**Data Sources** (`data_source.py`, `providers/`):
- Unified interface for multiple data providers (JQData, Tushare)
- JQData: Primary source for mainland Chinese stocks (excludes BJ after 2025-07)
- Tushare: Used for Beijing Stock Exchange (BJ) stocks
- Configurable exchange filtering

**API Layer** (`api.py`, `api_server.py`):
- `StockDataAPI`: Core Python API for direct integration
- `StockDataAPIServer`: REST API server with Flask
- Supports both programmatic access and HTTP endpoints

**Services** (`services/`):
- `UpdateService`: Handles data synchronization and updates
- `StockListService`: Manages stock metadata and filtering
- `TradeImportService`: Imports user trading records
- `PositionService`: Manages user position data

### Data Models

**Market Data**:
- `stock_list`: Stock metadata (code, name, exchange, industry)
- `price_data`: OHLCV data with adjustment factors
- `valuation_data`: PE, PB, market cap metrics
- `*_data`: Financial statements (income, balance sheet, cashflow)
- `indicator_data`: Calculated financial ratios

**User Data**:
- `user_transactions`: Trading history with fees and commissions
- `user_positions`: Daily position snapshots with P&L
- `user_account_info`: Account summary data

### Configuration

**Main Config** (`config.yaml`):
- Database path and connection settings
- Data source credentials and API limits
- Exchange filtering (excludes BJ stocks by default)
- Update schedules and batch sizes

**Environment Variables** (`.env`):
```
JQ_USERNAME=your_jqdata_username
JQ_PASSWORD=your_jqdata_password
TUSHARE_TOKEN=your_tushare_token
```

### Key Design Patterns

**Data Type Enumeration** (`data_source.py`):
```python
DataType.STOCK_LIST
DataType.PRICE_DATA
DataType.BALANCE_SHEET
DataType.INCOME_STATEMENT
# etc.
```

**Factory Pattern** (`api.py`):
```python
api = create_api(db_path="stock_data.duckdb")
```

**Service Layer**: Business logic separated into focused service classes

**Configuration Management**: YAML-based with environment variable overrides

### Important Notes

- **BJ Stock Exclusion**: As of 2025-07-01, JQData no longer maintains Beijing Stock Exchange data. The system filters these out by default using exchange filtering.
- **Incremental Updates**: The system supports both full historical updates and daily incremental updates.
- **Thread Safety**: Database operations are designed for concurrent access with proper connection management.
- **Data Validation**: Built-in data quality checks for price consistency and completeness.