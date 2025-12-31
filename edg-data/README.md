# EDG West Dataset

This directory contains data related to **EDG West**, a Distribution System Operator (DSO) operating in Western Bulgaria.

## Files

### `bankya.csv`

A CSV dataset containing energy measurement records.

**Columns:**

- `BUS_name`: Identifier for the bus/connection point (e.g., `SF_0004`).
- `timestamp`: Date and time of the measurement (ISO 8601 format).
- `measurement`: The type of energy measurement (e.g., `consumedEnergy`, `generatedEnergy`).
- `value`: The measured value.
- `unit`: The unit of measurement (typically `kWh`).

