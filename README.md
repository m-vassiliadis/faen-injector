# FAEN API Client

This directory contains the FAEN API client for retrieving energy data and processing it for the CDE server. The client supports multiple dataset types including building consumption, photovoltaic generation, weather data, and EV charging infrastructure.

## Supported Dataset Types

1. **Building Consumption** - Household energy consumption (hourly)
2. **Photovoltaic Generation** - Solar generation + weather data (hourly)
3. **MRAE Charging Infrastructure** - EV charging data (monthly aggregated)
4. **Combined** - Process multiple types simultaneously

## Key Files

- `main.py` - Main CLI for FAEN API and CDE integration
- `faen_client.py` - API client with authentication
- `cde_client.py` - CDE middleware client
- `data_utils.py` - Data transformation utilities
- `mrae.py` - MRAE-specific functionality
- `env.template` - Configuration template

## Setup

### Quick Setup (Recommended)

1. Run the setup script:
```bash
./setup.sh
```

2. Create your configuration file from the template:
```bash
vcp env.template .en
```

3. Edit `.env` with your actual credentials if needed:
```bash
# FAEN API Configuration
FAEN_API_URL=https://datacellarvcapi.test.ctic.es
FAEN_USERNAME=datacellar.developer
FAEN_PASSWORD=your_actual_password
CDE_API_URL=https://your-cde-url.com
```

4. Run the script:
```bash
source venv/bin/activate
python main.py
```

### Manual Setup

1. Create and activate virtual environment:
```bash
python3 -m venv venv
source venv/bin/activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

## Quarterly Data Extraction Script

The `run_quarterly_data_extraction.sh` script automatically processes FAEN data for all quarters from 2022-01-01 to 2024-12-31:

```bash
chmod +x run_quarterly_data_extraction.sh
./run_quarterly_data_extraction.sh
```

The script processes data in quarterly chunks using non-interactive mode.

### Alternative: Environment Variables

```bash
export FAEN_API_URL="https://datacellarvcapi.test.ctic.es"
export FAEN_USERNAME="datacellar.developer"
export FAEN_PASSWORD="your_actual_password"
```

## Usage

### Interactive Mode (Recommended for First-Time Users)

Run the script and follow the prompts:

```bash
python main.py
```

You'll be prompted to select:
- Dataset type (1-5)
- Date range
- Record limit

### Non-Interactive Mode

For automation and scripting, use command-line arguments:

```bash
# Building consumption data
python main.py --dataset-type 1 --start-date 2025-05-01 --end-date 2025-05-02 --non-interactive

# Photovoltaic generation + weather
python main.py --dataset-type 2 --start-date 2025-05-01 --end-date 2025-05-02 --non-interactive

# MRAE charging infrastructure
python main.py --dataset-type 4 --start-date 2020-01-01 --end-date 2023-12-31 --non-interactive

# All dataset types
python main.py --dataset-type 5 --start-date 2025-05-01 --end-date 2025-05-02 --non-interactive
```

### Dataset Type Options

| Option | Description                   | Data Sources             | Granularity |
| ------ | ----------------------------- | ------------------------ | ----------- |
| 1      | Building Consumption          | Energy consumption       | Hourly      |
| 2      | Photovoltaic Generation       | Solar + weather          | Hourly      |
| 3      | Both Consumption & Generation | Energy + solar + weather | Hourly      |
| 4      | MRAE Charging Infrastructure  | EV charging data         | Monthly     |
| 5      | All Types                     | All available datasets   | Mixed       |

### Command-Line Options

```bash
python main.py --help
```

Available options:
- `--dataset-type {1,2,3,4,5}` - Select dataset type
- `--start-date YYYY-MM-DD` - Start date (inclusive)
- `--end-date YYYY-MM-DD` - End date (exclusive)
- `--limit N` - Maximum records to retrieve
- `--location LOCATION` - Location filter for MRAE data (default: MRA-E)
- `--non-interactive` - Run without prompts (auto-confirm all)

### Workflow

The script automatically:
1. Authenticates with FAEN API (OAuth2)
2. Queries data for specified date range and type
3. Generates JSON-LD dataset definitions
4. Uploads datasets to CDE
5. Transforms and uploads datapoints in batches

## Authentication

The client uses OAuth2 password flow as shown in the Swagger UI:
- Content-Type: `application/x-www-form-urlencoded`
- Credentials: username, password, grant_type=password
- Returns: Bearer token for subsequent API calls

## MRAE Charging Infrastructure

The MRAE (Metropolitan Region Amsterdam Electric) dataset provides monthly aggregated EV charging data:

- **Total Energy** (kWh) - Energy consumed
- **Connection Time** (hours) - Connection duration
- **Electric Kilometers** (km) - Distance driven
- **CO2 Reduction** (tons) - Environmental impact
- **Charging Sessions** (count) - Number of sessions
- **Charging Poles** (count) - Active infrastructure

## API Endpoints

### Authentication
- `POST /token` - Get access token (OAuth2 password flow)

### Data Retrieval
- `POST /consumption/query` - Query consumption data with MongoDB-style queries
- `GET /generation/` - Query generation data with filters
- `GET /weather/` - Query weather data with filters
- `GET /mrae/` - Query MRAE charging infrastructure data
- `GET /mrae/stats` - Get MRAE dataset statistics
- `GET /mrae/monthly-summary/{year}` - Get MRAE monthly summary
- `GET /users/me/` - Get current user information

## Example Queries

### Consumption Data (MongoDB-style)

```python
query = {
    "datetime": {
        "$gte": {"$date": "2022-07-13T16:00:00+0200"},
        "$lte": {"$date": "2022-07-15T16:00:00+0200"}
    }
}
```

### MRAE Data (GET parameters)

```python
faen_client.query_mrae(
    start_date="2020-01-01",
    end_date="2023-12-31",
    location="MRA-E",
    limit=100
)
```

## Output Files

Generated files are saved to the `datasets/` directory:

```
datasets/
├── faen_consumption_dataset_definition_YYYY-MM-DD_to_YYYY-MM-DD.json
├── faen_generation_dataset_definition_YYYY-MM-DD_to_YYYY-MM-DD.json
├── faen_mrae_dataset_definition_YYYY-MM-DD_to_YYYY-MM-DD.json
└── test_mrae_output.json (from test script)
```
