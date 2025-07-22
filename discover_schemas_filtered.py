# discover_schemas_filtered.py
"""
This script connects to Snowflake to discover active tables within specified schemas,
filters them based on a whitelist and data freshness criteria, and outputs their
schema structure and sample data to a text file. It also logs tables that were
skipped and the reasons why.
"""

import sys
import logging
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
from typing import Optional, List, Dict, Any
import json
import snowflake.connector # Ensure this is imported for explicit connection handling

# Adjust path to import from project files dynamically.
# This ensures that modules like `utils` and `config` can be found.
project_root = Path(__file__).resolve().parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from utils import get_snowflake_connection
from config import TARGET_SCHEMAS

# --- Setup Logging ---
logger = logging.getLogger("SchemaDiscovery")
logger.setLevel(logging.INFO)
# Ensure the logger only has one handler to prevent duplicate messages.
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

# --- Configuration Constants ---
OUTPUT_FILENAME = "schema_discovery_filtered_output.txt"
SKIPPED_LOG_FILENAME = "skipped_tables_log.json"

# Define a whitelist of tables that should always be included in the discovery,
# regardless of their activity status. This is useful for critical tables
# that might not have recent updates but are still important to monitor.
WHITELISTED_TABLES = {
    "PRODUCT_PG.S3.buff_play_service_games",
    "PRODUCT_PG.S3.contest_challenge_contest_challenges_parquet",
    "PRODUCT_PG.S3.contest_leaderboard_contest_parquet",
    "PRODUCT_PG.S3.contest_leaderboard_country_group_parquet",
    "PRODUCT_PG.S3.mobile_buff_play_game_platform_details_parquet",
    "PRODUCT_PG.S3.mobile_buff_play_games_parquet"
}


# --- UTILITY FUNCTIONS ---

def get_date_column(cursor: snowflake.connector.cursor.SnowflakeCursor, db: str, schema: str, table: str, logger: logging.Logger, skipped_log: List[Dict[str, Any]]) -> Optional[str]:
    """
    Finds the most suitable date/timestamp column in a table using a predefined priority order.
    This column is used to determine table activity/freshness.

    Args:
        cursor: An active Snowflake cursor.
        db: The database name.
        schema: The schema name.
        table: The table name.
        logger: The logger instance for recording messages.
        skipped_log: A list to append log entries for skipped tables due to description failure.

    Returns:
        Optional[str]: The name of the most suitable date/timestamp column, or None if none is found
                       or the table cannot be described.
    """
    full_table_name = f"{db}.{schema}.{table}"
    try:
        # Attempt to describe the table to get column metadata.
        cursor.execute(f'DESCRIBE TABLE "{db}"."{schema}"."{table}"')
        cols = cursor.fetchall()
    except Exception as e:
        logger.warning(f"Could not describe table {full_table_name} to find date column. Skipping. Error: {e}")
        skipped_log.append({"table": full_table_name, "reason": "describe_table_failed", "error": str(e)})
        return None

    # Extract column names (uppercase for case-insensitive matching) and identify date/timestamp types.
    col_names_upper = [c[0].upper() for c in cols]
    date_type_cols = [c[0] for c in cols if "DATE" in c[1].upper() or "TIMESTAMP" in c[1].upper()]
    
    # Define a priority order for date/timestamp columns, based on common naming conventions.
    priority_order = ["UPDATED_AT", "UPDATEDAT", "CREATED_AT", "CREATEDAT", "TIMESTAMP", "EVENTTIMESTAMP", 
                      "ACTIVE_DATE", "INSTALL_DATE", "DATE", "DATE_PART", "REGISTERED_AT", "LAST_ACTIVE"] 
    
    # Iterate through the priority list to find the best match.
    for col_name_priority in priority_order:
        if col_name_priority in col_names_upper:
            # Find the original case-sensitive column name that matches the priority and is a date/timestamp type.
            original_name = next((c[0] for c in cols if c[0].upper() == col_name_priority and 
                                   ("DATE" in c[1].upper() or "TIMESTAMP" in c[1].upper())), None)
            if original_name:
                return original_name

    # If no high-priority column is found, but there are other date/timestamp columns, pick the first one.
    if date_type_cols:
        logger.info(f"Using '{date_type_cols[0]}' as date column for {full_table_name} (no high-priority column found).")
        return date_type_cols[0]

    # If no suitable date/timestamp column is found at all, return None.
    return None


def is_table_active(cursor: snowflake.connector.cursor.SnowflakeCursor, db: str, schema: str, table: str, date_col: str, logger: logging.Logger, skipped_log: List[Dict[str, Any]]) -> bool:
    """
    Checks if a table has received data within the last 30 days based on its designated date column.
    This determines the 'freshness' or 'activity' of the table.

    Args:
        cursor: An active Snowflake cursor.
        db: The database name.
        schema: The schema name.
        table: The table name.
        date_col: The column used for checking activity (e.g., 'UPDATED_AT').
        logger: The logger instance for recording messages.
        skipped_log: A list to append log entries for skipped tables due to activity check failure.

    Returns:
        bool: True if the table has data within the last 30 days, False otherwise.
    """
    one_month_ago = (datetime.utcnow() - timedelta(days=30)).strftime('%Y-%m-%d %H:%M:%S')
    full_table_name = f"{db}.{schema}.{table}"
    query = f'SELECT MAX("{date_col}") FROM "{db}"."{schema}"."{table}"'
    try:
        # Execute the query to get the maximum date, with a timeout.
        cursor.execute(query, timeout=30) 
        max_date = cursor.fetchone()[0]

        # If MAX(date_col) returns NULL, the table is considered empty for activity purposes.
        if max_date is None:
            logger.info(f"Skipping table {full_table_name}: No data found for activity check.")
            return False
        
        # Convert to pandas datetime with error coercion for robust parsing of various formats.
        pd_max_date_raw = pd.to_datetime(max_date, errors='coerce') 
        # If parsing results in NaT (Not a Time), it means the date was unparseable.
        if pd.isna(pd_max_date_raw):
             logger.warning(f"Could not parse max date '{max_date}' for table {full_table_name}. Assuming inactive for safety.")
             skipped_log.append({"table": full_table_name, "reason": "unparseable_max_date", "value": str(max_date)})
             return False

        # Localize to None to remove timezone info, making comparison consistent with `one_month_ago`.
        pd_max_date = pd_max_date_raw.tz_localize(None) 
        
        # Log the raw date string for debugging, especially if it's not a datetime object.
        log_date_str = str(max_date) if not isinstance(max_date, datetime) else max_date.strftime('%Y-%m-%d %H:%M:%S')

        # Compare the max date with the 30-day threshold.
        if pd_max_date >= pd.to_datetime(one_month_ago).tz_localize(None):
            logger.info(f"Table {full_table_name} is active (last update: {log_date_str}).")
            return True
        else:
            logger.info(f"Skipping table {full_table_name}: Last update ({log_date_str}) is older than 30 days.")
            skipped_log.append({"table": full_table_name, "reason": "stale_table_no_updates_in_30d", "last_update": log_date_str})
            return False
            
    except snowflake.connector.errors.ProgrammingError as e:
        # Catch Snowflake-specific errors (e.g., query timeout, column not found, permissions).
        logger.warning(f"Could not check activity for {full_table_name} on date column '{date_col}'. Assuming inactive. Error: {e}")
        # Add to skipped log with detailed error info.
        skipped_log.append({"table": full_table_name, "reason": "activity_check_query_failed", "error": str(e)})
        return False
    except Exception as e:
        # Catch any other unexpected errors during the activity check.
        logger.error(f"An unexpected error occurred during activity check for {full_table_name}: {e}")
        skipped_log.append({"table": full_table_name, "reason": "activity_check_unexpected_error", "error": str(e)})
        return False

def get_sample_data(conn: snowflake.connector.SnowflakeConnection, fqn: str, num_rows: int = 3) -> Optional[pd.DataFrame]:
    """
    Fetches sample data (first N rows) from a given table using pandas.

    Args:
        conn: An active Snowflake connection object.
        fqn: The fully qualified name of the table.
        num_rows: The number of rows to fetch as a sample.

    Returns:
        Optional[pd.DataFrame]: A pandas DataFrame containing the sample data, or None if
                                fetching fails.
    """
    db, schema, table = fqn.split('.')
    quoted_fqn = f'"{db}"."{schema}"."{table}"' # Quote identifiers for Snowflake.
    query = f'SELECT * FROM {quoted_fqn} LIMIT {num_rows}'
    try:
        df = pd.read_sql(query, conn) # Use pandas to read SQL query results into a DataFrame.
        return df
    except Exception as e:
        logger.error(f"Failed to fetch sample data for {fqn}. Error: {e}")
        return None

# --- MAIN DISCOVERY LOGIC ---

def discover_active_schemas():
    """
    Connects to Snowflake, iterates through predefined target schemas,
    identifies active tables (based on whitelist or recent data),
    describes their schema, fetches sample data, and writes this information
    to a text file. It also maintains a log of skipped tables with reasons.
    """
    conn = get_snowflake_connection()
    if not conn:
        logger.critical("Could not connect to Snowflake. Aborting discovery.")
        return

    cursor = conn.cursor()
    # List to store detailed reasons for skipping tables.
    skipped_log_entries = []
    logger.info(f"Starting ACTIVE schema discovery... Output will be saved to '{OUTPUT_FILENAME}'. Skipped tables log to '{SKIPPED_LOG_FILENAME}'.")

    # Set pandas display options to ensure full DataFrame content is printed
    # without truncation when writing to the output file.
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000) 
    pd.set_option('display.max_rows', None) 

    try:
        # Open the output file for writing the discovered schema and sample data.
        with open(OUTPUT_FILENAME, "w", encoding="utf-8") as output_file:
            output_file.write("--- START OF FILTERED SCHEMA DISCOVERY OUTPUT (ACTIVE TABLES & SAMPLE DATA) ---\n\n")

            # Iterate through each database and schema pair defined in TARGET_SCHEMAS.
            for db, schema in TARGET_SCHEMAS:
                logger.info(f"--- Discovering Schema: {db}.{schema} ---")
                try:
                    # List all tables in the current schema.
                    cursor.execute(f'SHOW TABLES IN SCHEMA "{db}"."{schema}"')
                    tables = cursor.fetchall()
                    if not tables:
                        logger.warning(f"No tables found in {db}.{schema}. Skipping.")
                        skipped_log_entries.append({"table": f"{db}.{schema}.*", "reason": "no_tables_in_schema"})
                        continue

                    for table_row in tables:
                        table_name = table_row[1] # Extract table name from SHOW TABLES output.
                        full_table_name = f"{db}.{schema}.{table_name}"
                        
                        # Check if the table is explicitly whitelisted.
                        is_whitelisted = full_table_name in WHITELISTED_TABLES

                        include_table = False
                        if is_whitelisted:
                            logger.info(f"Including whitelisted table: {full_table_name}")
                            include_table = True
                        else:
                            # If not whitelisted, perform activity check.
                            # The `skipped_log_entries` list is passed to `get_date_column` and `is_table_active`
                            # functions to allow them to append specific skip reasons if they encounter issues.
                            temp_skipped_log_for_internal_fns = [] # Use a temp list here to capture internal logs easily.
                            date_col = get_date_column(cursor, db, schema, table_name, logger, temp_skipped_log_for_internal_fns) 
                            
                            if date_col:
                                # If a date column is found, check activity.
                                if is_table_active(cursor, db, schema, table_name, date_col, logger, temp_skipped_log_for_internal_fns):
                                    include_table = True
                                else:
                                    # If not active, the reason is already captured by is_table_active.
                                    # We just need to append them to the main skipped log.
                                    skipped_log_entries.extend(temp_skipped_log_for_internal_fns)
                                    continue # Skip to the next table if not active.
                            else:
                                # If no date column is found, log it and skip.
                                skipped_log_entries.append({"table": full_table_name, "reason": "no_date_column_found"})
                                continue # Skip to the next table.


                        if include_table:
                            # If the table is active or whitelisted, write its details to the output file.
                            output_file.write(f"--- Table: {full_table_name} ---\n")
                            # Describe table (schema)
                            try:
                                cursor.execute(f'DESCRIBE TABLE "{db}"."{schema}"."{table_name}"')
                                columns = cursor.fetchall()
                                output_file.write("Schema:\n")
                                for col_row in columns:
                                    col_name = col_row[0]
                                    col_type = col_row[1]
                                    output_file.write(f"- {col_name} ({col_type})\n")
                                output_file.write("\n")

                                # Fetch and write sample data for the table.
                                sample_df = get_sample_data(conn, full_table_name, num_rows=3)
                                if sample_df is not None and not sample_df.empty:
                                    output_file.write(f"Sample Data (First 3 Rows):\n")
                                    # Using to_string() ensures all columns are displayed.
                                    output_file.write(sample_df.to_string(index=False))
                                    output_file.write("\n\n")
                                elif sample_df is not None and sample_df.empty:
                                    output_file.write("Sample Data: Table is empty or no rows found.\n\n")
                                else:
                                    output_file.write("Sample Data: Could not retrieve sample data.\n\n")

                            except Exception as e:
                                # Log and capture errors if describing table or fetching sample data fails.
                                output_file.write(f"  -> ERROR: Could not describe table or fetch sample data for {full_table_name}. Reason: {e}\n\n")
                                skipped_log_entries.append({"table": full_table_name, "reason": "describe_or_sample_failed", "error": str(e)})
                
                except Exception as e:
                    # Log and capture fatal errors if an entire schema cannot be accessed.
                    error_msg = f"--- FATAL ERROR: Could not access schema {db}.{schema}. Reason: {e} ---\n\n"
                    logger.error(error_msg)
                    skipped_log_entries.append({"table": f"{db}.{schema}.*", "reason": "schema_access_failed", "error": str(e)})
                    output_file.write(error_msg)
                    continue

            output_file.write("--- END OF FILTERED SCHEMA DISCOVERY OUTPUT ---\n")

    except Exception as e:
        # Catch any errors during file writing operations.
        logger.critical(f"An error occurred during file operations: {e}")
    finally:
        # Ensure the Snowflake connection and cursor are closed.
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        logger.info(f"Discovery complete. Output saved to '{OUTPUT_FILENAME}'.")

        # --- Write skipped tables to a JSON log file ---
        if skipped_log_entries:
            try:
                with open(SKIPPED_LOG_FILENAME, 'w', encoding='utf-8') as f:
                    json.dump(skipped_log_entries, f, indent=4)
                logger.info(f"Skipped tables log saved to '{SKIPPED_LOG_FILENAME}'.")
            except Exception as e:
                logger.error(f"Failed to write skipped tables log to JSON file: {e}")

            # --- Print a summary of skipped tables to console ---
            logger.info("\n--- Discovery Summary ---")
            df = pd.DataFrame(skipped_log_entries)
            reason_counts = df['reason'].value_counts()
            logger.info(f"Total tables skipped: {len(skipped_log_entries)}")
            logger.info("Reasons for skipping:\n%s", reason_counts.to_string())
        else:
            logger.info("No tables were skipped during discovery.")


# --- Script Entry Point ---
if __name__ == "__main__":
    discover_active_schemas()