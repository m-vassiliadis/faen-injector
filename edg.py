#!/usr/bin/env python3
"""
EDG West (Bankya, Bulgaria) Module

This module encapsulates all EDG West-specific functionality including:
- CSV data loading with date filtering and aggregation
- Dataset definition generation
- Data transformation to CDE format

Designed for maximum decoupling from the main application.
Unlike MRAE/FAEN, EDG uses local CSV files instead of remote APIs.
"""

import csv
import os
import uuid
from collections import defaultdict
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Union

from console_utils import (
    print_data,
    print_error,
    print_info,
    print_section,
    print_success,
    print_warning,
)


class EDGDataLoader:
    """Loader for EDG West CSV data with aggregation support"""

    # Default CSV path relative to project root
    DEFAULT_CSV_PATH = "edg-data/bankya.csv"

    # Bankya, Bulgaria coordinates
    BANKYA_LATITUDE = 42.72
    BANKYA_LONGITUDE = 23.17

    def __init__(self, csv_path: Optional[str] = None):
        """
        Initialize the EDG data loader

        Args:
            csv_path: Path to the CSV file (default: edg-data/bankya.csv)
        """
        self.csv_path = csv_path or self.DEFAULT_CSV_PATH
        self._cached_data: Optional[List[Dict[str, Any]]] = None

    def load_csv(
        self,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> List[Dict[str, Any]]:
        """
        Load and parse CSV data with optional date filtering

        Args:
            start_date: Start date for filtering (inclusive)
            end_date: End date for filtering (exclusive)

        Returns:
            List of records as dictionaries with keys:
            - BUS_name: str
            - timestamp: str (ISO format)
            - measurement: str (consumedEnergy or generatedEnergy)
            - value: float
            - unit: str
        """
        print_section("Loading EDG West CSV Data")
        print_data("CSV path", self.csv_path, 1)

        if not os.path.exists(self.csv_path):
            print_error(f"CSV file not found: {self.csv_path}")
            raise FileNotFoundError(f"CSV file not found: {self.csv_path}")

        records = []
        skipped_count = 0

        with open(self.csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)

            for row in reader:
                try:
                    # Parse timestamp
                    timestamp_str = row["timestamp"]
                    record_date = datetime.fromisoformat(
                        timestamp_str.replace("Z", "")
                    ).date()

                    # Apply date filtering
                    if start_date and record_date < start_date:
                        skipped_count += 1
                        continue
                    if end_date and record_date >= end_date:
                        skipped_count += 1
                        continue

                    # Parse value
                    value = float(row["value"])

                    records.append(
                        {
                            "BUS_name": row["BUS_name"],
                            "timestamp": timestamp_str,
                            "measurement": row["measurement"],
                            "value": value,
                            "unit": row["unit"],
                        }
                    )
                except (ValueError, KeyError) as e:
                    print_warning(f"Skipping invalid row: {e}")
                    skipped_count += 1
                    continue

        print_success(f"Loaded {len(records)} records from CSV")
        if skipped_count > 0:
            print_info(f"Skipped {skipped_count} records (filtered or invalid)")

        return records

    def aggregate_by_timestamp(
        self, records: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Aggregate values across all buses by timestamp and measurement type

        Args:
            records: List of raw CSV records

        Returns:
            List of aggregated records with keys:
            - timestamp: str (ISO format with Z suffix)
            - consumedEnergy: float (sum across all buses)
            - generatedEnergy: float (sum across all buses)
        """
        print_info("Aggregating data across all buses...")

        # Group by timestamp
        aggregated: Dict[str, Dict[str, float]] = defaultdict(
            lambda: {"consumedEnergy": 0.0, "generatedEnergy": 0.0}
        )

        for record in records:
            timestamp = record["timestamp"]
            measurement = record["measurement"]
            value = record["value"]

            # Normalize timestamp to include Z suffix
            if not timestamp.endswith("Z"):
                if "T" not in timestamp:
                    timestamp = timestamp + "T00:00:00Z"
                else:
                    timestamp = timestamp + "Z"

            if measurement in aggregated[timestamp]:
                aggregated[timestamp][measurement] += value

        # Convert to list format
        result = []
        for timestamp in sorted(aggregated.keys()):
            result.append(
                {
                    "timestamp": timestamp,
                    "consumedEnergy": aggregated[timestamp]["consumedEnergy"],
                    "generatedEnergy": aggregated[timestamp]["generatedEnergy"],
                }
            )

        print_success(f"Aggregated into {len(result)} monthly records")
        return result

    def get_aggregated_data(
        self,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> List[Dict[str, Any]]:
        """
        Load, filter, and aggregate data in one call

        Args:
            start_date: Start date for filtering (inclusive)
            end_date: End date for filtering (exclusive)

        Returns:
            List of aggregated records
        """
        records = self.load_csv(start_date, end_date)
        return self.aggregate_by_timestamp(records)

    def get_statistics(self) -> Dict[str, Any]:
        """
        Get dataset statistics

        Returns:
            Dictionary with statistics:
            - bus_count: Number of unique buses
            - date_range: (min_date, max_date)
            - total_consumption: Sum of all consumedEnergy values
            - total_generation: Sum of all generatedEnergy values
            - record_count: Total number of records
        """
        records = self.load_csv()

        buses = set()
        dates = []
        total_consumption = 0.0
        total_generation = 0.0

        for record in records:
            buses.add(record["BUS_name"])
            dates.append(
                datetime.fromisoformat(record["timestamp"].replace("Z", "")).date()
            )

            if record["measurement"] == "consumedEnergy":
                total_consumption += record["value"]
            elif record["measurement"] == "generatedEnergy":
                total_generation += record["value"]

        return {
            "bus_count": len(buses),
            "date_range": (min(dates), max(dates)) if dates else (None, None),
            "total_consumption": total_consumption,
            "total_generation": total_generation,
            "record_count": len(records),
        }


class EDGDatasetGenerator:
    """Generator for EDG West dataset definitions"""

    # Field configuration: (id, name, description, unit, averagable, summable, anonymizable)
    FIELD_DEFINITIONS = [
        (
            1,
            "consumedEnergy",
            "Aggregated energy consumption from EDG West Bankya distribution grid in kWh",
            "kWh",
            False,
            True,
            False,
        ),
        (
            2,
            "generatedEnergy",
            "Aggregated energy generation from EDG West Bankya distribution grid in kWh",
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
        aggregated_data: Optional[List[Dict[str, Any]]] = None,
        location: str = "Bankya, Bulgaria",
    ) -> Dict[str, Any]:
        """
        Generate dataset definition for EDG West Bankya data

        Creates a 2-field dataset with:
        - Total aggregated energy consumption (kWh)
        - Total aggregated energy generation (kWh)

        Uses datacellar:EnergyMeter as timeSeriesMetadata type
        Includes GeoLocalizedDataset metadata with Bankya coordinates

        Args:
            start_date: Start date (date or datetime object)
            end_date: End date (date or datetime object)
            aggregated_data: List of aggregated records (optional, for validation)
            location: Location identifier (default: "Bankya, Bulgaria")

        Returns:
            Dataset definition dictionary in JSON-LD format
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
            title = f"EDG West Bankya Energy {start_year}"
        else:
            title = f"EDG West Bankya Energy {start_year}-{end_year}"

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

        print_info(f"Creating EDG West dataset for location: {location}")
        print_info(f"Date range: {start_date} to {end_date}")

        # Create timeseries entries (one per field)
        timeseries_entries = []

        for (
            field_id,
            field_name,
            field_desc,
            unit,
            averagable,
            summable,
            anonymizable,
        ) in EDGDatasetGenerator.FIELD_DEFINITIONS:
            timeseries_guid = str(uuid.uuid4())

            timeseries_entry = {
                "@type": "datacellar:TimeSeries",
                "@id": f"http://datacellar.org/timeseries/{timeseries_guid}",
                "datacellar:timeSeriesId": timeseries_guid,
                "datacellar:datasetFieldID": field_id,
                "datacellar:startDate": timeseries_start,
                "datacellar:endDate": timeseries_end,
                "datacellar:timeZone": "0",
                "datacellar:granularity": EDGDatasetGenerator.MONTHLY_GRANULARITY,
                "datacellar:latitude": EDGDataLoader.BANKYA_LATITUDE,
                "datacellar:longitude": EDGDataLoader.BANKYA_LONGITUDE,
                "datacellar:dataPoints": [],
                "datacellar:timeSeriesMetadata": {
                    "@type": "datacellar:EnergyMeter",
                    "datacellar:deviceID": "edg_west_bankya_aggregated",
                    "datacellar:loadType": "aggregate",
                },
            }
            timeseries_entries.append(timeseries_entry)

        print_info(
            f"Created {len(timeseries_entries)} timeseries for EDG West dataset"
        )

        # Dataset definition
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
                "datacellar:latitude": {"@type": "xsd:float"},
                "datacellar:longitude": {"@type": "xsd:float"},
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
            "datacellar:description": f"Monthly aggregated energy consumption and generation data from EDG West {location} distribution grid covering the period from {start_date.isoformat()} to {end_date.isoformat()}. Data is aggregated across all bus/connection points in the grid.",
            "datacellar:datasetSelfDescription": {
                "@type": "datacellar:DatasetDescription",
                "datacellar:datasetDescriptionID": 1,
                "datacellar:datasetMetadataTypes": [
                    "datacellar:GeoLocalizedDataset",
                    "datacellar:Installation",
                ],
                "datacellar:datasetFields": [
                    {
                        "@type": "datacellar:DatasetField",
                        "datacellar:datasetFieldID": field_id,
                        "datacellar:name": field_name,
                        "datacellar:description": field_desc,
                        "datacellar:timeseriesMetadataType": "datacellar:EnergyMeter",
                        "datacellar:fieldType": {
                            "@type": "datacellar:FieldType",
                            "datacellar:unit": unit,
                            "datacellar:averagable": averagable,
                            "datacellar:summable": summable,
                            "datacellar:anonymizable": anonymizable,
                        },
                    }
                    for field_id, field_name, field_desc, unit, averagable, summable, anonymizable in EDGDatasetGenerator.FIELD_DEFINITIONS
                ],
            },
            "datacellar:timeSeries": timeseries_entries,
            "datacellar:datasetMetadata": [
                {
                    "@type": "datacellar:GeoLocalizedDataset",
                    "datacellar:latitude": EDGDataLoader.BANKYA_LATITUDE,
                    "datacellar:longitude": EDGDataLoader.BANKYA_LONGITUDE,
                },
                {
                    "@type": "datacellar:Installation",
                    "datacellar:installationType": "distributionGrid",
                    "datacellar:capacity": None,
                    "datacellar:capacityUnit": "kW",
                },
            ],
        }

        return dataset_definition


class EDGDataTransformer:
    """Transformer for converting aggregated EDG data to CDE datapoint format"""

    # Field mapping: measurement name -> (CDE field name, measurement name, unit)
    FIELD_MAPPINGS = {
        "consumedEnergy": ("consumedEnergy", "consumedEnergy", "kWh"),
        "generatedEnergy": ("generatedEnergy", "generatedEnergy", "kWh"),
    }

    @staticmethod
    def transform_to_datapoints(
        aggregated_data: List[Dict[str, Any]],
        timeseries_mapping: Dict[str, str],
    ) -> List[Dict[str, Any]]:
        """
        Transform aggregated EDG data into CDE datapoint format

        Args:
            aggregated_data: List of aggregated records with timestamp and values
            timeseries_mapping: Dictionary mapping field names to timeseries IDs
                {"consumedEnergy": "uuid-1", "generatedEnergy": "uuid-2"}

        Returns:
            List of datapoint dictionaries ready for CDE API
        """
        datapoints = []
        skipped_records = 0

        print_info(
            f"Transforming {len(aggregated_data)} EDG records to CDE datapoints"
        )

        for record in aggregated_data:
            timestamp = record.get("timestamp")

            # Skip records with missing timestamp
            if not timestamp:
                skipped_records += 1
                continue

            # Ensure timestamp has Z suffix
            if not timestamp.endswith("Z"):
                if "T" not in timestamp:
                    timestamp = timestamp + "T00:00:00Z"
                else:
                    timestamp = timestamp + "Z"

            # Create datapoints for each measurement type
            for measurement_name, (
                cde_field,
                measurement,
                unit,
            ) in EDGDataTransformer.FIELD_MAPPINGS.items():
                value = record.get(measurement_name)

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
        print_success(f"Transformed {len(datapoints)} EDG datapoints")
        if skipped_records > 0:
            print_warning(f"Skipped {skipped_records} records with missing data")

        return datapoints

    @staticmethod
    def create_timeseries_mapping(
        timeseries_list: List[Dict[str, Any]],
    ) -> Dict[str, str]:
        """
        Create mapping from EDG field names to CDE timeseries IDs

        Uses datasetFieldID to map:
        - Field ID 1 -> consumedEnergy
        - Field ID 2 -> generatedEnergy

        Args:
            timeseries_list: List of timeseries from CDE

        Returns:
            Dictionary mapping field names to timeseries IDs
        """
        timeseries_mapping = {}

        # Field ID to field name mapping
        field_map = {
            1: "consumedEnergy",
            2: "generatedEnergy",
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
                        f"{field_map[field_id]} -> {ts_id}",
                        2,
                    )

        # Validate we have all required mappings
        expected_fields = list(field_map.values())
        missing_fields = [f for f in expected_fields if f not in timeseries_mapping]
        if missing_fields:
            print_warning(f"Missing mappings for: {', '.join(missing_fields)}")

        return timeseries_mapping


# Convenience functions for backward compatibility
def generate_edg_dataset_definition(
    start_date: Union[date, datetime],
    end_date: Union[date, datetime],
    aggregated_data: Optional[List[Dict[str, Any]]] = None,
    location: str = "Bankya, Bulgaria",
) -> Dict[str, Any]:
    """
    Convenience function for generating EDG dataset definitions

    This function provides backward compatibility with the original implementation.
    """
    return EDGDatasetGenerator.generate_dataset_definition(
        start_date, end_date, aggregated_data, location
    )


def transform_edg_to_datapoints(
    aggregated_data: List[Dict[str, Any]],
    timeseries_mapping: Dict[str, str],
) -> List[Dict[str, Any]]:
    """
    Convenience function for transforming EDG data to datapoints

    This function provides backward compatibility with the original implementation.
    """
    return EDGDataTransformer.transform_to_datapoints(
        aggregated_data, timeseries_mapping
    )
