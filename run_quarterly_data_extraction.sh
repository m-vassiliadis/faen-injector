#!/bin/bash

# Script to sequentially call main.py with various date ranges
# This script uses non-interactive mode to avoid prompts

set -e # Exit on any error

# Parse command line arguments
DRY_RUN=false
DATASET_TYPE=3 # Default dataset type is 3 (Both Consumption and Generation)
while [[ $# -gt 0 ]]; do
	case $1 in
	--dry-run)
		DRY_RUN=true
		shift
		;;
	--dataset-type | -d)
		DATASET_TYPE="$2"
		shift 2
		;;
	*)
		echo "Unknown option: $1"
		exit 1
		;;
	esac
done

if [ "$DRY_RUN" = true ]; then
	echo "Dry-run mode: Will print commands without executing them"
	echo "======================================================"
else
	echo "Starting batch data extraction"
	echo "=============================="
fi

# Define the date ranges
# Format: "START_DATE END_DATE"
# Note: main.py treats end_date as exclusive for query logic, but we pass the dates as is.
# The script usually expects [start, end).
intervals=(
	# 2022 Quarters
	"2022-01-01 2022-03-31"
	"2022-04-01 2022-06-30"
	"2022-07-01 2022-09-30"
	"2022-10-01 2022-12-31"

	# 2023 Quarters
	"2023-01-01 2023-03-31"
	"2023-04-01 2023-06-30"
	"2023-07-01 2023-09-30"
	"2023-10-01 2023-12-31"

	# 2023 Full Year
	"2023-01-01 2024-01-01"

	# 2024 Quarters
	"2024-01-01 2024-03-31"
	"2024-04-01 2024-06-30"
	"2024-07-01 2024-09-30"
	"2024-10-01 2024-12-31"

	# 2024 Full Year
	"2024-01-01 2025-01-01"
)

echo "Using dataset type: $DATASET_TYPE"
case $DATASET_TYPE in
1) echo "Dataset type: Consumption only" ;;
2) echo "Dataset type: Generation + Weather" ;;
3) echo "Dataset type: Both (Consumption and Generation)" ;;
4) echo "Dataset type: MRAE Charging" ;;
5) echo "Dataset type: All types" ;;
*) echo "Unknown dataset type: $DATASET_TYPE" ;;
esac
echo ""

# Process each interval
for i in "${!intervals[@]}"; do
	# Split the interval into start and end dates
	read -r start_date end_date <<<"${intervals[i]}"

	echo "Processing interval $((i + 1)): $start_date to $end_date"

	# Build the command
	# Increased limit to 50000 to accommodate annual datasets (8760 hours * 4 = ~35k for 15min data)
	cmd="python3 main.py \
        --dataset-type $DATASET_TYPE \
        --start-date \"$start_date\" \
        --end-date \"$end_date\" \
        --limit 50000 \
        --non-interactive"

	if [ "$DRY_RUN" = true ]; then
		echo "Would execute: $cmd"
	else
		# Call main.py with non-interactive mode
		eval $cmd
		echo "Completed interval $((i + 1)): $start_date to $end_date"
		echo "------------------------------------------------------------"
		sleep 1
	fi
done

if [ "$DRY_RUN" = false ]; then
	echo "All batch data extractions completed successfully!"
fi
