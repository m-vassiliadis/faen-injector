#!/usr/bin/env python3
"""
MRAE (Metropolitan Region Amsterdam Electric) Module

This module encapsulates all MRAE-specific functionality including:
- API client methods for querying MRAE data
- Dataset definition generation
- Data transformation to CDE format
- Schema compliance utilities

Designed for maximum decoupling from the main application.
"""

import uuid
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Union
from urllib.parse import urljoin

import requests

from console_utils import (
    print_data,
    print_error,
    print_info,
    print_section,
    print_success,
    print_warning,
)


class MRAEClient:
    """Client for interacting with MRAE charging infrastructure endpoints"""

    def __init__(self, session: requests.Session, base_url: str):
        """
        Initialize the MRAE client

        Args:
            session: Authenticated requests session
            base_url: Base URL of the FAEN API
        """
        self.session = session
        self.base_url = base_url.rstrip("/")

    def query_mrae(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        location: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Query MRAE charging infrastructure data using GET request

        Args:
            start_date: Start date in YYYY-MM-DD format (optional)
            end_date: End date in YYYY-MM-DD format (optional)
            location: Location filter (e.g., "MRA-E") (optional)
            limit: Maximum number of results to return (optional)

        Returns:
            List of MRAE charging infrastructure records
        """
        print_section("🔌 Querying MRAE Charging Data")
        mrae_url = urljoin(self.base_url + "/", "mrae/")
        print_data("Endpoint", mrae_url, 1)

        # Build query parameters
        params = {}
        if start_date:
            params["start_date"] = start_date
            print_data("Start date", start_date, 1)
        if end_date:
            params["end_date"] = end_date
            print_data("End date", end_date, 1)
        if location:
            params["location"] = location
            print_data("Location", location, 1)
        if limit:
            params["limit"] = limit
            print_data("Limit", str(limit), 1)

        try:
            print_info("Sending query request...")
            response = self.session.get(mrae_url, params=params)
            response.raise_for_status()

            data = response.json()
            record_count = len(data) if isinstance(data, list) else 1
            print_success(f"✓ Retrieved {record_count} MRAE records")
            return data

        except requests.exceptions.RequestException as e:
            print_error(f"Failed to query MRAE data: {e}")
            if hasattr(e, "response") and e.response is not None:
                print_data("Response status", str(e.response.status_code), 1)
                print_data("Response content", e.response.text[:200], 1)
            raise

    def get_mrae_stats(self) -> Dict[str, Any]:
        """
        Get MRAE dataset statistics

        Returns:
            Dictionary with MRAE statistics
        """
        print_section("📊 MRAE Statistics")
        stats_url = urljoin(self.base_url + "/", "mrae/stats")
        print_data("Endpoint", stats_url, 1)

        try:
            print_info("Fetching MRAE statistics...")
            response = self.session.get(stats_url)
            response.raise_for_status()

            stats = response.json()
            print_success("✓ MRAE statistics retrieved")

            # Display key statistics
            if isinstance(stats, dict):
                for key, value in stats.items():
                    print_data(key, str(value), 1)

            return stats

        except requests.exceptions.RequestException as e:
            print_error(f"Failed to get MRAE stats: {e}")
            if hasattr(e, "response") and e.response is not None:
                print_data("Response status", str(e.response.status_code), 1)
                print_data("Response content", e.response.text[:200], 1)
            raise

    def get_mrae_monthly_summary(self, year: int) -> List[Dict[str, Any]]:
        """
        Get MRAE monthly summary for a specific year

        Args:
            year: Year to retrieve data for

        Returns:
            List of MRAE records for all months in the year
        """
        print_section(f"📅 MRAE Monthly Summary for {year}")
        summary_url = urljoin(self.base_url + "/", f"mrae/monthly-summary/{year}")
        print_data("Endpoint", summary_url, 1)

        try:
            print_info(f"Fetching MRAE data for {year}...")
            response = self.session.get(summary_url)
            response.raise_for_status()

            data = response.json()
            record_count = len(data) if isinstance(data, list) else 1
            print_success(f"✓ Retrieved {record_count} monthly records for {year}")
            return data

        except requests.exceptions.RequestException as e:
            print_error(f"Failed to get MRAE monthly summary: {e}")
            if hasattr(e, "response") and e.response is not None:
                print_data("Response status", str(e.response.status_code), 1)
                print_data("Response content", e.response.text[:200], 1)
            raise


class MRAEDatasetGenerator:
    """Generator for MRAE dataset definitions"""

    # Field configuration: (id, name, description, unit, averagable, summable, anonymizable)
    # Simplified to use only allowed field name from CDE measurements.csv
    FIELD_DEFINITIONS = [
        (
            1,
            "consumedEnergy",
            "Total energy consumed by electric vehicle charging in kWh",
            "kWh",
            False,
            True,
            False,
        ),
    ]

    # Monthly granularity in seconds (30 days * 24 hours * 3600 seconds)
    MONTHLY_GRANULARITY = 2592000.0

    @staticmethod
    def generate_dataset_definition(
        start_date: Union[date, datetime],
        end_date: Union[date, datetime],
        mrae_data: List[Dict[str, Any]] = None,
        location: str = "MRA-E",
    ) -> Dict[str, Any]:
        """
        Generate dataset definition for MRAE charging infrastructure data

        Creates a multi-field dataset with:
        - Total energy consumption (kWh)
        - Total connection time (hours)
        - Electric kilometers driven
        - CO2 reduction (tons)
        - Number of charging sessions
        - Charging infrastructure metrics

        Uses datacellar:EVChargingStation as timeSeriesMetadata type

        Important:
        - Each TimeSeries must include both @id (URI) and datacellar:timeSeriesId (UUID)
        - EVChargingStation metadata should NOT include latitude/longitude for privacy
        - Only Installation metadata is required (GeoLocalizedDataset is optional)

        Args:
            start_date: Start date (date or datetime object)
            end_date: End date (date or datetime object)
            mrae_data: List of MRAE records (optional, for validation)
            location: Location identifier (default: "MRA-E")

        Returns:
            Dataset definition dictionary in JSON-LD format with 6 fields
        """
        # Convert to date objects if datetime objects are passed
        if isinstance(start_date, datetime):
            start_date = start_date.date()
        if isinstance(end_date, datetime):
            end_date = end_date.date()

        # Generate title based on date range
        start_year = start_date.year
        end_year = end_date.year

        if start_year == end_year:
            title = f"MRAE Charging Infrastructure {start_year}"
        else:
            title = f"MRAE Charging Infrastructure {start_year}-{end_year}"

        # Create ISO datetime strings for the time series
        timeseries_start = (
            datetime.combine(start_date, datetime.min.time()).isoformat() + "Z"
        )
        timeseries_end = (
            datetime.combine(end_date, datetime.max.time())
            .replace(microsecond=0)
            .isoformat()
            + "Z"
        )

        print_info(f"Creating MRAE dataset for location: {location}")
        print_info(f"Date range: {start_date} to {end_date}")

        # Create 6 timeseries entries (one per field)
        timeseries_entries = []

        for (
            field_id,
            field_name,
            field_desc,
            unit,
            averagable,
            summable,
            anonymizable,
        ) in MRAEDatasetGenerator.FIELD_DEFINITIONS:
            timeseries_guid = str(uuid.uuid4())

            timeseries_entry = {
                "@type": "datacellar:TimeSeries",
                "@id": f"http://datacellar.org/timeseries/{timeseries_guid}",
                "datacellar:timeSeriesId": timeseries_guid,
                "datacellar:datasetFieldID": field_id,
                "datacellar:startDate": timeseries_start,
                "datacellar:endDate": timeseries_end,
                "datacellar:timeZone": "0",
                "datacellar:granularity": MRAEDatasetGenerator.MONTHLY_GRANULARITY,
                "datacellar:dataPoints": [],
                "datacellar:timeSeriesMetadata": {
                    "@type": "datacellar:EVChargingStation",
                    "datacellar:serviceProvider": location,
                    "datacellar:networkOperator": "Metropolitan Region Amsterdam Electric",
                    "datacellar:address": "Amsterdam Metropolitan Region, Netherlands",
                },
            }
            timeseries_entries.append(timeseries_entry)

        print_info(f"Created {len(timeseries_entries)} timeseries for MRAE dataset (consumedEnergy only)")

        # Dataset definition with 6 field types
        dataset_definition = {
            "@context": {
                "id": "@id",
                "type": "@type",
                "graph": "@graph",
                "datacellar": "http://datacellar.org/schema#",
                "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
                "sh": "http://www.w3.org/ns/shacl#",
                "xsd": "http://www.w3.org/2001/XMLSchema#",
                "datacellar:installedCapacity": {"@type": "xsd:float"},
                "datacellar:elevation": {"@type": "xsd:float"},
                "datacellar:floorArea": {"@type": "xsd:float"},
                "datacellar:insulationSurface": {"@type": "xsd:float"},
                "datacellar:openingsArea": {"@type": "xsd:float"},
                "datacellar:orientation": {"@type": "xsd:float"},
                "datacellar:startDate": {"@type": "xsd:dateTime"},
                "datacellar:endDate": {"@type": "xsd:dateTime"},
                "datacellar:tilt": {"@type": "xsd:float"},
                "datacellar:timestamp": {"@type": "xsd:dateTime"},
                "datacellar:totalAnnualEnergyConsumption": {"@type": "xsd:float"},
                "datacellar:value": {"@type": "xsd:float"},
                "datacellar:granularity": {"@type": "xsd:float"},
            },
            "@type": "datacellar:Dataset",
            "datacellar:name": title,
            "datacellar:description": f"Monthly aggregated electric vehicle charging infrastructure data from {location} covering the period from {start_date.isoformat()} to {end_date.isoformat()}",
            "datacellar:datasetSelfDescription": {
                "@type": "datacellar:DatasetDescription",
                "datacellar:datasetDescriptionID": 1,
                "datacellar:datasetMetadataTypes": ["datacellar:Installation"],
                "datacellar:datasetFields": [
                    {
                        "@type": "datacellar:DatasetField",
                        "datacellar:datasetFieldID": field_id,
                        "datacellar:name": field_name,
                        "datacellar:description": field_desc,
                        "datacellar:timeseriesMetadataType": "datacellar:EVChargingStation",
                        "datacellar:fieldType": {
                            "@type": "datacellar:FieldType",
                            "datacellar:unit": unit,
                            "datacellar:averagable": averagable,
                            "datacellar:summable": summable,
                            "datacellar:anonymizable": anonymizable,
                        },
                    }
                    for field_id, field_name, field_desc, unit, averagable, summable, anonymizable in MRAEDatasetGenerator.FIELD_DEFINITIONS
                ],
            },
            "datacellar:timeSeries": timeseries_entries
        }

        return dataset_definition


class MRAEDataTransformer:
    """Transformer for converting MRAE data to CDE datapoint format"""

    # Field mapping: MRAE API field -> (CDE field name, measurement name, unit)
    # Simplified to map only energy consumption to allowed CDE field name
    FIELD_MAPPINGS = {
        "total_kwh": ("consumedEnergy", "consumedEnergy", "kWh"),
    }

    @staticmethod
    def transform_to_datapoints(
        mrae_data: List[Dict[str, Any]], timeseries_mapping: Dict[str, str]
    ) -> List[Dict[str, Any]]:
        """
        Transform MRAE data into CDE datapoint format

        Maps MRAE fields to timeseries:
        - total_kwh -> totalEnergy timeseries
        - total_connection_time -> connectionTime timeseries
        - total_electric_kilometers -> electricKilometers timeseries
        - co2_reduction -> co2Reduction timeseries
        - total_sessions -> chargingSessions timeseries
        - charging_poles -> chargingPoles timeseries

        Args:
            mrae_data: List of MRAE records
            timeseries_mapping: Dictionary mapping field names to timeseries IDs

        Returns:
            List of datapoint dictionaries ready for CDE API
        """
        datapoints = []
        skipped_records = 0

        print_info(f"Transforming {len(mrae_data)} MRAE records to CDE datapoints")

        for record in mrae_data:
            period = record.get("period")
            location = record.get("location")

            # Skip records with missing period
            if not period:
                skipped_records += 1
                continue

            # Convert period to ISO timestamp (first day of month at 00:00:00 UTC)
            try:
                # Period is in YYYY-MM-DD format
                timestamp = period
                if not timestamp.endswith("Z"):
                    # Ensure it's a full datetime with time component
                    if "T" not in timestamp:
                        timestamp = timestamp + "T00:00:00Z"
                    elif not timestamp.endswith("Z") and not timestamp.endswith(
                        "+00:00"
                    ):
                        timestamp = timestamp + "Z"
            except (ValueError, AttributeError):
                print_warning(f"⚠ Invalid period format: {period}")
                skipped_records += 1
                continue

            # Create datapoints for each field
            for mrae_field, (
                cde_field,
                measurement,
                unit,
            ) in MRAEDataTransformer.FIELD_MAPPINGS.items():
                value = record.get(mrae_field)

                # Skip null values
                if value is None:
                    continue

                # Get the corresponding timeseries ID
                timeseries_id = timeseries_mapping.get(cde_field)
                if not timeseries_id:
                    continue

                datapoint = {
                    "measurement": measurement,
                    "unit": unit,
                    "value": float(value),
                    "timestamp": timestamp,
                    "timeseries_id": timeseries_id,
                }

                datapoints.append(datapoint)

        # Print summary
        print_success(f"✓ Transformed {len(datapoints)} MRAE datapoints")
        if skipped_records > 0:
            print_warning(f"⚠ Skipped {skipped_records} records with missing data")

        return datapoints

    @staticmethod
    def create_timeseries_mapping(
        timeseries_list: List[Dict[str, Any]],
    ) -> Dict[str, str]:
        """
        Create mapping from MRAE field names to CDE timeseries IDs

        Args:
            timeseries_list: List of timeseries from CDE

        Returns:
            Dictionary mapping field names to timeseries IDs
        """
        timeseries_mapping = {}

        # Field ID to field name mapping (updated for simplified dataset)
        field_map = {
            1: "consumedEnergy",
        }

        for ts in timeseries_list:
            # Get datasetFieldID from the timeseries
            dataset_field = ts.get("datasetField", {})
            field_id = dataset_field.get("datacellar:datasetFieldID")
            ts_id = ts.get("id")

            if field_id and ts_id:
                if field_id in field_map:
                    timeseries_mapping[field_map[field_id]] = ts_id
                    print_data(
                        f"Mapped field {field_id}",
                        f"{field_map[field_id]} → {ts_id}",
                        2,
                    )

        # Validate we have all required mappings
        expected_fields = list(field_map.values())
        missing_fields = [f for f in expected_fields if f not in timeseries_mapping]
        if missing_fields:
            print_warning(f"⚠ Missing mappings for: {', '.join(missing_fields)}")

        return timeseries_mapping


# Convenience functions for backward compatibility
def generate_mrae_dataset_definition(
    start_date: Union[date, datetime],
    end_date: Union[date, datetime],
    mrae_data: List[Dict[str, Any]] = None,
    location: str = "MRA-E",
) -> Dict[str, Any]:
    """
    Convenience function for generating MRAE dataset definitions

    This function provides backward compatibility with the original implementation.
    """
    return MRAEDatasetGenerator.generate_dataset_definition(
        start_date, end_date, mrae_data, location
    )


def transform_mrae_to_datapoints(
    mrae_data: List[Dict[str, Any]], timeseries_mapping: Dict[str, str]
) -> List[Dict[str, Any]]:
    """
    Convenience function for transforming MRAE data to datapoints

    This function provides backward compatibility with the original implementation.
    """
    return MRAEDataTransformer.transform_to_datapoints(mrae_data, timeseries_mapping)
