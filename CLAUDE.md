# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is the FAEN API client directory within the CDE (Data Cellar) server project. FAEN is a Spanish energy consumption data provider, and this client retrieves consumption data and integrates it with the CDE internal API for storage and processing.

## Core Architecture

### Main Components

- **FAEN API Client (`FaenApiClient`)**: Handles OAuth2 authentication and data retrieval from the FAEN API
  - Supports consumption, generation, and weather data endpoints
  - OAuth2 password flow authentication
  - MongoDB-style query support
- **CDE API Client (`CDEApiClient`)**: Manages dataset uploads and datapoint submissions to the CDE internal API
- **Dataset Generation**: Transforms FAEN data into CDE-compatible JSON-LD dataset definitions
  - Support for consumption-only datasets (legacy)
  - Support for combined generation + weather datasets (new)
- **Data Transformation**: Converts FAEN records to CDE datapoint format for timeseries storage
  - Generation data: `generation_kwh` → `generatedEnergy` timeseries
  - Weather data: `ta` → `outdoorTemperature`, `hr` → `humidityLevel` timeseries
- **EDG West Module (`edg.py`)**: Local CSV-based data integration for Bulgarian DSO
  - Loads data from local CSV file (no remote API required)
  - Aggregates data across all bus/connection points
  - Combined dataset with `consumedEnergy` and `generatedEnergy` fields

### API Integration Flow

1. **Authentication**: OAuth2 password flow with FAEN API using form-encoded credentials
2. **Data Retrieval**: MongoDB-style queries to fetch consumption data from FAEN
3. **Dataset Creation**: Generate JSON-LD dataset definitions from retrieved data
4. **CDE Upload**: POST dataset definitions and individual datapoints to CDE API
5. **Timeseries Mapping**: Map FAEN data to CDE timeseries using dataset field information for datapoint association

## Common Commands

### Running the Client
```bash
# Run the main client with interactive CLI (default)
python main.py

# Alternative: using virtual environment
source venv/bin/activate
python main.py

# Non-interactive mode with command line arguments
python main.py --non-interactive --dataset-type 1 --start-date 2025-01-01 --end-date 2025-01-02 --limit 10
```

### Configuration Setup
```bash
# Create local configuration
cp config_example.env .env
# Edit .env with actual credentials
```

### Testing Environment Loading
```bash
cd data/FAEN
python test_dotenv_behavior.py
```

## Environment Configuration

### Configuration Priority Order
1. **Local `.env` file** (data/FAEN/.env) - Highest priority
2. **Environment variables** 
3. **Parent directory .env files** (searched upward)

### Required Environment Variables
- `FAEN_API_URL`: Base URL for FAEN API (e.g., https://datacellarvcapi.test.ctic.es)
- `FAEN_USERNAME`: Username for FAEN API authentication 
- `FAEN_PASSWORD`: Password for FAEN API authentication
- `CDE_API_URL`: Base URL for CDE internal API (default: http://localhost:5000)

### Configuration Notes
- Script automatically removes `/docs` suffix from FAEN_API_URL if present
- Local .env file takes precedence over all other configuration sources
- Script works when run from any directory but loads its own .env file first

## Data Flow Architecture

### FAEN Data Structure
- **Consumption Records**: User ID, datetime, energy consumption (kWh)
- **Generation Records**: User ID, datetime, energy generation (kWh), type, nominal power
- **Weather Records**: Station-based, datetime_utc, temperature (ta), humidity (hr), and other meteorological parameters
- **MongoDB Queries**: Date range filtering using MongoDB Extended JSON format
- **Authentication**: Bearer token required for all data endpoints

### CDE Integration
- **Dataset Definitions**: JSON-LD format with datacellar namespace
  - Consumption datasets: Single measurement type (`consumedEnergy`)
  - Combined datasets: Multiple measurement types (`generatedEnergy`, `outdoorTemperature`, `humidityLevel`)
- **Timeseries**: Individual timeseries per user/measurement with metadata
  - PVPanel metadata with latitude/longitude coordinates from weather data
  - Field-based mapping using `datasetField` information from CDE API
- **Datapoints**: Individual measurements with timestamps and values embedded in timeseries
  - Complete CSV files (one per timeseries/measurement type) saved to `datasets/csv/` folder
  - Filename format: `{dataset_name}_{measurement_type}_{start_date}_to_{end_date}.csv`
  - Example: `FAEN_Consumption_May_2025_consumedEnergy_2025-05-01_to_2025-06-01.csv`
  - CSV files contain all datapoints for a single measurement type across entire time range
  - Uploads still happen in configurable batches for efficiency
  - CSV files are automatically gitignored via the `datasets/` pattern
- **Health Checks**: CDE API health endpoint for connectivity verification

## CLI Features

### Interactive and Non-Interactive Modes

The CLI supports two operation modes:

#### **Interactive Mode** (Default)
- Provides step-by-step prompts for user input
- Interactive dataset selection, date range, and limit configuration
- Confirmation prompts at each major step
- Ideal for manual operation and debugging

#### **Non-Interactive Mode** (`--non-interactive`)
- Accepts all configuration via command line arguments
- Auto-confirms all prompts (automatically answers "Y" to all confirmations)
- Ideal for automated scripts, CI/CD pipelines, and batch processing
- Required arguments: `--dataset-type`, `--start-date`, `--end-date`
- Optional arguments: `--limit`

### Command Line Arguments

```bash
python main.py [OPTIONS]

Options:
  --dataset-type {1,2,3,4,5,6}  Dataset type to process:
                                  1 = Building Consumption
                                  2 = Photovoltaic Generation (+ Weather)
                                  3 = Both Types (separate datasets)
                                  4 = MRAE Charging Infrastructure
                                  5 = All types
                                  6 = EDG West Bankya (from local CSV)
  --start-date YYYY-MM-DD       Start date (inclusive)
  --end-date YYYY-MM-DD         End date (exclusive)
  --limit N                     Maximum number of records to retrieve
  --non-interactive             Run without interactive prompts (auto-confirm)
  --location LOCATION           Location filter for MRAE data (default: MRA-E)
  --edg-csv-path PATH           Path to EDG CSV file (default: edg-data/bankya.csv)
  -h, --help                    Show help message and exit
```

### Dataset Types

1. **Building Consumption**: Energy consumption data from buildings
2. **Photovoltaic Generation**: Combined generation + weather data (generation, temperature, humidity)
3. **Both Types**: Creates separate datasets for both consumption and generation data
4. **MRAE Charging Infrastructure**: EV charging infrastructure data from Amsterdam region
5. **All Types**: Creates separate datasets for all types (1-4)
6. **EDG West Bankya**: Bulgarian DSO energy data from local CSV file (aggregated consumption + generation)

### Date Range Configuration  
- **Default Range**: 2025-05-01 to 2025-06-01 (May 2025, 31 days) - Interactive mode only
- **Inclusive Start**: Start date includes the specified day from 00:00:00
- **Exclusive End**: End date excludes the specified day (stops at 00:00:00 of end date)
- **Custom Ranges**: 
  - Interactive mode: Users prompted for input in YYYY-MM-DD format
  - Non-interactive mode: Specified via `--start-date` and `--end-date` arguments
- **Date Validation**: Automatic validation and correction (swaps dates if start > end)

### Multi-Dataset Processing
- Parallel processing of multiple dataset types
- **Interactive Mode**: Individual confirmation prompts for each step
- **Non-Interactive Mode**: Auto-confirmation of all steps with visual indicators
- Comprehensive progress tracking and error reporting
- Separate upload and datapoint processing for each dataset type

## Development Notes

### API Endpoints Used
- **FAEN**: 
  - `/token` - OAuth2 authentication
  - `POST /consumption/query` - Consumption data (JSON body)
  - `GET /generation/` - Generation data (URL parameters)
  - `GET /weather/` - Weather data (URL parameters)
  - `/users/me/` - User information
- **CDE**: `/api/health`, `/api/dataset`, `/api/timeseries`

### Error Handling
- Comprehensive error reporting with colored console output
- **Interactive Mode**: User confirmation prompts at critical steps with proper exit handling
- **Non-Interactive Mode**: Automatic error handling and continuation
- Script exits cleanly if CDE API is unreachable or FAEN authentication fails
- Graceful handling of Ctrl+C interruption at any point
- Batch processing with individual failure tracking
- All "No" responses to confirmation prompts result in clean exit
- Non-interactive mode validates all arguments before execution

### Timeseries Field Mapping
- **Field-Based Mapping**: Uses `datasetField` information from CDE `/api/timeseries` endpoint
- **Primary Mapping**: Maps by `datacellar:datasetFieldID` (1=generation, 2=temperature, 3=humidity)
- **Fallback Mapping**: Maps by `datacellar:name` if field ID is unavailable
- **Order Independent**: Works regardless of timeseries order returned by CDE API
- **Validation**: Ensures all required mappings are present before processing datapoints

### Dataset Schema Updates
- **Temperature Unit**: Changed from "C" to "Celsius" for better consistency
- **Field Names**: Updated temperature field from "temperature" to "outdoorTemperature"
- **PVPanel Metadata**: Uses latitude/longitude coordinates instead of deviceID
- **Weather Queries**: Uses `datetime_utc` field instead of `datetime` for weather data

### Non-Interactive Mode Features
- **Auto-Confirmation**: All Y/n prompts automatically answered with "Y"
- **Visual Indicators**: Non-interactive operations marked with "🤖 [NON-INTERACTIVE]" prefix
- **Argument Validation**: Validates required arguments and date formats before execution
- **Graceful Fallback**: Falls back to interactive mode if non-interactive arguments are incomplete
- **Batch Friendly**: Suitable for automation, scripting, and CI/CD pipelines

### Usage Examples

```bash
# Interactive mode - consumption data
python main.py
# (Answers prompts interactively)

# Non-interactive - consumption data only
python main.py --non-interactive --dataset-type 1 --start-date 2025-01-01 --end-date 2025-01-02

# Non-interactive - generation + weather data with custom limit
python main.py --non-interactive --dataset-type 2 --start-date 2025-05-01 --end-date 2025-05-03 --limit 5

# Non-interactive - both dataset types with large date range
python main.py --non-interactive --dataset-type 3 --start-date 2025-01-01 --end-date 2025-12-31 --limit 1000

# Non-interactive - MRAE charging infrastructure data
python main.py --non-interactive --dataset-type 4 --start-date 2020-01-01 --end-date 2023-12-31

# Non-interactive - EDG West Bankya data from local CSV
python main.py --non-interactive --dataset-type 6 --start-date 2022-01-01 --end-date 2022-12-31

# EDG West with custom CSV path
python main.py --non-interactive --dataset-type 6 --start-date 2022-01-01 --end-date 2022-12-31 --edg-csv-path /path/to/custom.csv

# View help
python main.py --help
```

## EDG West Data Integration

### Overview

EDG West is a Distribution System Operator (DSO) operating in Western Bulgaria. Unlike FAEN and MRAE which use remote APIs, EDG data is loaded from local CSV files included in the repository.

### Data Source

**File**: `edg-data/bankya.csv`

**Columns**:
- `BUS_name`: Identifier for the bus/connection point (e.g., `SF_0004`)
- `timestamp`: Date and time of the measurement (ISO 8601 format)
- `measurement`: The type of energy measurement (`consumedEnergy`, `generatedEnergy`)
- `value`: The measured value
- `unit`: The unit of measurement (typically `kWh`)

### Data Characteristics

- **Granularity**: Monthly aggregated data
- **Time Range**: 2022-01-01 to 2022-12-31 (full year 2022)
- **Buses**: 85 unique bus/connection points
- **Measurements**: `consumedEnergy` and `generatedEnergy`
- **Aggregation**: Values are summed across all buses into 2 total timeseries

### Geographic Metadata

- **Location**: Bankya, Bulgaria
- **Coordinates**: 42.72°N, 23.17°E

### Module Structure

The EDG implementation (`edg.py`) follows the same pattern as MRAE:

- **`EDGDataLoader`**: Loads and aggregates CSV data
  - `load_csv()`: Parse CSV with date filtering
  - `aggregate_by_timestamp()`: Sum values across all buses
  - `get_aggregated_data()`: Combined load and aggregate

- **`EDGDatasetGenerator`**: Creates JSON-LD dataset definitions
  - 2 fields: `consumedEnergy` (fieldID=1), `generatedEnergy` (fieldID=2)
  - Metadata type: `datacellar:EnergyMeter`
  - Monthly granularity (2592000 seconds)

- **`EDGDataTransformer`**: Transforms data to CDE datapoint format
  - `transform_to_datapoints()`: Convert aggregated records
  - `create_timeseries_mapping()`: Map field IDs to timeseries IDs


