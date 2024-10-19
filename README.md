# Precisely Data Enrichment And Analysis

This project provides a set of tools to enrich datasets using the Precisely API. It includes functionality to fetch and process data from various Precisely datasets, including demographics, flood risk, coastal risk, property attributes, parcel data, and building information.

## Features

- Fetch demographic data (PsyteGeodemographics)
- Retrieve flood risk and coastal risk information
- Enhance property data with additional attributes
- Secure authentication handling with automatic token refresh
- Data sampling capabilities
- Progress tracking with tqdm

## Components

1. `demographicsDataPrecisely`: Handles API calls for demographic, flood risk, and coastal risk data.
2. `dataProcessorForDemographics`: Processes and combines the data retrieved from the API.
3. `propertyDataPrecisely`: Manages API calls for property, parcel, and building data.

## Prerequisites

- Python 3.x
- Required Python packages: pandas, requests, tqdm, requests, json, threading, base64

## Installation

1. Clone this repository:
   ```
   git clone https://github.com/yourusername/precisely-data-enrichment.git
   cd precisely-data-enrichment
   ```

2. Install the required packages:
   ```
   pip install pandas requests tqdm
   ```

## Usage

### For Demographic, Flood Risk, and Coastal Risk Data

```python
from precisely_data_enrichment import demographicsDataPrecisely, dataProcessorForDemographics

client_id = "your_client_id"
client_secret = "your_client_secret"
df = pd.read_csv('your_data.csv')

precisely_api = demographicsDataPrecisely(client_id, client_secret)
results = precisely_api.process_dataframe(df)

processor = dataProcessorForDemographics(results)
combined_df = processor.get_dataframe()
processor.save_to_csv('output_demographics.csv')
```

### For Property, Parcel, and Building Data

```python
from precisely_data_enrichment import propertyDataPrecisely

client_id = "your_client_id"
client_secret = "your_client_secret"
df = pd.read_csv('your_data.csv')

api = propertyDataPrecisely(client_id, client_secret, sample_percentage=100)
sampled_df = api.sample_data(df)
enhanced_df = api.enhance_data(sampled_df)
enhanced_df.to_csv("enriched_property_data.csv", index=False)
```

## API Queries

The Precisely API is queried using GraphQL. The project includes several predefined queries for retrieving data:

1. **Psyte Geodemographics**
2. **Coastal Risk**
3. **Flood Risk**

These queries are sent to the Precisely API using the `get_response` method.

| Query Type | Description |
|------------|-------------|
| `psyteGeodemographics` | Retrieves demographic categories, census data, income, and property types. |
| `coastalRisk` | Retrieves information about the nearest coast and potential risk factors. |
| `floodRisk` | Retrieves flood zone and risk details, including FEMA flood zones. |

## Project Classes

### `demographicsDataPrecisely`

This class handles API token generation, token expiration management, and querying of demographic and risk data from Precisely.

| Method | Description |
|--------|-------------|
| `generate_auth_token()` | Retrieves a new authentication token from the Precisely API. |
| `refresh_token()` | Refreshes the token when expired and updates headers. |
| `process_dataframe(df)` | Processes a pandas DataFrame and retrieves data for each `PBKEY`. |
| `generate_psyteGeodemographics_query()` | Builds the GraphQL query for demographic data. |
| `generate_coastalRisk_query()` | Builds the GraphQL query for coastal risk data. |
| `generate_floodRisk_query()` | Builds the GraphQL query for flood risk data. |
| `get_response(query)` | Executes a query and retrieves results from Precisely API. |

### `dataProcessorForDemographics`

Processes the results from the API and combines them into a structured DataFrame.

| Method | Description |
|--------|-------------|
| `extract_data(result)` | Extracts psyte, coastal, and flood data from API responses. |
| `flatten_and_prefix(data, prefix)` | Flattens nested JSON data and adds a prefix to column names. |
| `process_single_result(result)` | Processes a single result and combines it into a DataFrame row. |
| `create_combined_dataframe()` | Combines all API results into a single DataFrame. |
| `save_to_csv(filename)` | Exports the combined DataFrame to a CSV file. |

### `propertyDataPrecisely`

Handles property data retrieval, token management, and data enhancement from Precisely.

| Method | Description |
|--------|-------------|
| `get_new_token()` | Retrieves a new authentication token. |
| `refresh_token()` | Refreshes the token after expiration. |
| `fetch_data(query)` | Executes a GraphQL query and fetches results from the Precisely API. |
| `get_data(query, *path)` | Retrieves specific data using a query and nested path from the response. |

## Output Data Format

The final output is saved as a CSV file. The table below describes some of the key columns in the output file:

| Column Name | Description |
|-------------|-------------|
| `precisely_id` | Unique identifier for the property or location in Precisely's dataset. |
| `psyte_PSYTECategoryCode` | PSYTE category classification for the location. |
| `coastal_nearestWaterbodyName` | Name of the nearest water body. |
| `flood_floodZoneBaseFloodElevationFeet` | Elevation in feet for the base flood zone. |

## Security Note

This project requires a Precisely API client ID and secret. Never commit these credentials to your repository. Use environment variables or a secure secrets management system in production environments.

## Disclaimer

This project is not officially associated with Precisely. Use of the Precisely API is subject to Precisely's terms of service.
