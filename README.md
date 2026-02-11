Cloud Tenants QMS Import File

This script combines two CSVs into one, merging the data from both. It standardizes names, addresses, phone numbers, and filters out known bad email values.

The final output is a single CSV intended for import into QMS.

At a high level, this script:
- Loads the primary and alternate tenant CSV files
- Normalizes and maps fields into the QMS schema
- Cleans invalid or placeholder email addresses
- Combines middle + last names
- Selects the “best” phone number (cell/home/work)
- Merges alternate contact data using LegacyTenantId as the KEY
- Adds helper columns for duplicate detection
- Writes a QMS-formatted CSV output

Update these constants at the top of the script before running:

TENANTS_FILE = r"path/to/tenants.xlsx"
ALTERNATE_TENANTS_FILE = r"path/to/alternate_tenants.xlsx"
OUTPUT_FILE = r"path/to/output.csv"
SOURCE_SHEET_NAME = 0  # Excel sheet index or name

Requirements:

Python 3.10+
pandas

Install dependencies:

pip install pandas

Running the Script
python3 cloud_tenants.py

On success, you’ll see:

Written QMS-formatted data to <OUTPUT_FILE>
