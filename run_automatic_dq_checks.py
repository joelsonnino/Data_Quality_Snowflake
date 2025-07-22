# run_automatic_dq_checks.py
"""
This script orchestrates the automatic data quality checks against Snowflake tables.
It discovers active tables, generates data quality rules based on predefined conventions,
executes these rules, and compiles the results into a JSON report.
"""

import os
import sys
import json
import time
from datetime import datetime, timedelta
import logging
from pathlib import Path
import pandas as pd
from typing import List, Dict, Any, Optional
import snowflake.connector

# --- Setup Paths & Logging ---
# Resolve the project root dynamically to ensure imports work correctly
# regardless of where the script is run from within the project.
project_root = Path(__file__).resolve().parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# Import shared utility functions and configurations
from utils import get_snowflake_connection
from config import TARGET_SCHEMAS
# Import the module responsible for generating convention-based DQ rules
from automatic_dq_rules import generate_rules_for_table 

# Configure logging for better visibility into the script's execution
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', stream=sys.stdout)

# Define output directory and file for DQ reports
OUTPUT_DIR = project_root / "dq_reports"
OUTPUT_DIR.mkdir(exist_ok=True) # Create the directory if it doesn't exist
RESULTS_FILE = OUTPUT_DIR / "dq_results.json"

# Define a timeout for individual Snowflake queries to prevent indefinite hangs
QUERY_TIMEOUT_SECONDS = 30 

# --- Core Functions ---

def get_date_column(cursor: snowflake.connector.cursor.SnowflakeCursor, db: str, schema: str, table: str) -> Optional[str]:
    """
    Finds the most suitable date/timestamp column in a table for activity checks.
    It prioritizes common update/creation timestamp columns.

    Args:
        cursor: An active Snowflake cursor.
        db: The database name.
        schema: The schema name.
        table: The table name.

    Returns:
        str: The name of the most suitable date/timestamp column, or None if none is found.
    """
    try:
        # Describe the table to get column metadata (name and type)
        cursor.execute(f'DESCRIBE TABLE "{db}"."{schema}"."{table}"')
        cols = cursor.fetchall()
    except Exception as e:
        logging.debug(f"Could not describe table {db}.{schema}.{table} to find date column. Error: {e}")
        return None

    # Extract column names (uppercase for case-insensitive matching) and identify date/timestamp types.
    col_names = [c[0].upper() for c in cols]
    date_type_cols = [c[0] for c in cols if "DATE" in c[1].upper() or "TIMESTAMP" in c[1].upper()]
    
    # Define a priority order for common date/timestamp columns.
    # This list is ordered by how likely a column is to represent the 'last update' or 'creation' time.
    priority_order = ["UPDATED_AT", "UPDATEDAT", "CREATED_AT", "CREATEDAT", "TIMESTAMP", "EVENTTIMESTAMP", 
                      "DATE", "DATE_PART", "ACTIVE_DATE", "INSTALL_DATE", "REGISTERED_AT", "LAST_ACTIVE"] 
    
    # Iterate through the priority list to find the best date column.
    for col_name_priority in priority_order:
        if col_name_priority in col_names:
            # Find the original case-sensitive column name
            original_name = next(c[0] for c in cols if c[0].upper() == col_name_priority)
            # Ensure the found column also has a date/timestamp data type
            if original_name.upper() in [d.upper() for d in date_type_cols]:
                return original_name

    # If no high-priority column is found, return the first available date/timestamp column.
    return date_type_cols[0] if date_type_cols else None

def is_table_active(cursor: snowflake.connector.cursor.SnowflakeCursor, fqn: str, date_col: str) -> bool:
    """
    Checks if a table has recent data (within the last 30 days) based on a specified date column.

    Args:
        cursor: An active Snowflake cursor.
        fqn: The fully qualified name of the table (e.g., "DATABASE.SCHEMA.TABLE").
        date_col: The name of the date/timestamp column to check for activity.

    Returns:
        bool: True if the table has data from the last 30 days, False otherwise.
    """
    thirty_days_ago = (datetime.utcnow() - timedelta(days=30)).strftime('%Y-%m-%d %H:%M:%S')
    db, schema, table = fqn.split('.')
    # Query to find the maximum date in the specified date column.
    query = f'SELECT MAX("{date_col}") FROM "{db}"."{schema}"."{table}"'
    
    try:
        cursor.execute(query, timeout=15) # Use a short timeout for this quick check
        max_date = cursor.fetchone()[0]
        
        # If MAX(date_col) is NULL, the table is empty or the column is entirely null.
        if max_date is None:
            return False
        
        # Convert the fetched max_date to pandas datetime for robust comparison,
        # handling various date/timestamp formats and stripping timezone info.
        # Compare with the 'thirty_days_ago' threshold.
        return pd.to_datetime(max_date).tz_localize(None) >= pd.to_datetime(thirty_days_ago).tz_localize(None)
    except Exception as e:
        # Log a warning if the activity check fails (e.g., column not found, permissions).
        logging.warning(f"Could not check activity for {fqn} on date column '{date_col}'. Assuming inactive. Error: {e}")
        return False

def discover_active_tables(conn: snowflake.connector.SnowflakeConnection) -> List[Dict[str, str]]:
    """
    Scans predefined target schemas in Snowflake to identify and list active tables.
    An 'active' table is defined as one having a suitable date column and recent data (last 30 days).

    Args:
        conn: An active Snowflake connection object.

    Returns:
        List[Dict[str, str]]: A list of dictionaries, each containing the fully qualified
                              name ('fqn') and the primary date column ('date_col') for active tables.
    """
    cursor = conn.cursor()
    active_tables_info = []
    logging.info("--- Starting Discovery of Active Tables ---")
    
    # Iterate through each database and schema pair defined in config.py
    for db, schema in TARGET_SCHEMAS:
        logging.info(f"Scanning schema: {db}.{schema}")
        try:
            # Fetch all tables within the current schema.
            cursor.execute(f'SHOW TABLES IN SCHEMA "{db}"."{schema}"')
            for table_row in cursor.fetchall():
                table_name = table_row[1] # Table name is typically the second element in SHOW TABLES output
                fqn = f"{db}.{schema}.{table_name}" # Construct fully qualified name
                
                # Identify a suitable date column for the table.
                date_col = get_date_column(cursor, db, schema, table_name)
                
                if date_col:
                    # If a date column is found, check if the table is active.
                    if is_table_active(cursor, fqn, date_col):
                        logging.info(f"  -> Found ACTIVE table: {fqn} (Date Col: {date_col})")
                        active_tables_info.append({"fqn": fqn, "date_col": date_col})
                    else:
                        logging.info(f"  -> Skipping stale table: {fqn}")
                else:
                    logging.info(f"  -> Skipping table with no suitable date column for activity check: {fqn}")
        except Exception as e:
            # Log errors if a schema cannot be accessed or processed.
            logging.error(f"Could not process schema {db}.{schema}. Error: {e}")
            
    logging.info(f"--- Discovery Complete: Found {len(active_tables_info)} active tables. ---")
    cursor.close()
    return active_tables_info

def get_table_columns(conn: snowflake.connector.SnowflakeConnection, fqn: str) -> List[Dict[str, Any]]:
    """
    Describes a table and returns its column names and types.

    Args:
        conn: An active Snowflake connection object.
        fqn: The fully qualified name of the table.

    Returns:
        List[Dict[str, Any]]: A list of dictionaries, each representing a column
                              with 'name' and 'type' keys, or an empty list if description fails.
    """
    db, schema, table = fqn.split('.')
    cursor = conn.cursor()
    try:
        # Execute DESCRIBE TABLE to get column metadata.
        cursor.execute(f'DESCRIBE TABLE "{db}"."{schema}"."{table}"')
        cols = cursor.fetchall()
        # Format the results into a list of dictionaries.
        return [{'name': c[0], 'type': c[1]} for c in cols]
    except Exception as e:
        logging.error(f"Could not describe table {fqn}. Error: {e}")
        return []
    finally:
        cursor.close()

def generate_all_rules(conn: snowflake.connector.SnowflakeConnection, active_tables_info: List[Dict[str, str]], check_yesterday_only: bool) -> list:
    """
    Generates data quality rules for all discovered active tables using the
    convention-based rule generator (`automatic_dq_rules.py`).

    Args:
        conn: An active Snowflake connection.
        active_tables_info: A list of active table information (fqn, date_col).
        check_yesterday_only: A boolean flag to determine if tests should
                              filter for data from yesterday only.

    Returns:
        list: A list of rule dictionaries, formatted for execution.
    """
    all_rules = []
    logging.info("--- Generating Automatic DQ Rules for Active Tables ---")
    for table_info in active_tables_info:
        fqn = table_info['fqn']
        date_col = table_info['date_col'] # The primary date column for time-windowing
        
        # Get detailed column information for rule generation.
        columns = get_table_columns(conn, fqn)
        if columns:
            # Call the external rule generator to get tests for the current table.
            # Pass the primary_date_column and check_yesterday_only flag to the generator.
            rules_for_model = generate_rules_for_table(fqn, columns, date_col, check_yesterday_only)
            # Only add the model if it has actual tests generated.
            if rules_for_model.get('tests'):
                all_rules.append(rules_for_model)
    logging.info(f"--- Rule Generation Complete: Created tests for {len(all_rules)} tables. ---")
    return all_rules

def generate_test_sql(test_type: str, model_name: str, column_name: str, config: dict = None) -> str:
    """
    Generates a SQL query to count failing rows for a given data quality test.
    The query is dynamically constructed based on test type and configuration.

    Args:
        test_type: The type of data quality test (e.g., 'not_null', 'unique', 'custom_sql').
        model_name: The fully qualified name of the table being tested.
        column_name: The name of the column being tested (or 'N/A' for table-level tests).
        config: A dictionary of test-specific configuration, including date windowing.

    Returns:
        str: The generated SQL query, or None if the test type is unknown.
    """
    config = config or {} # Ensure config is always a dict
    
    # Safely quote database, schema, and table names for Snowflake compatibility
    db, schema, table = model_name.split('.')
    quoted_model_name = f'"{db}"."{schema}"."{table}"'
    
    # Safely quote column name (even if it's 'N/A' for table-level custom_sql)
    quoted_column_name = f'"{column_name}"' if column_name != 'N/A' else None

    base_condition_content = "" # This will hold the specific condition for the test type (e.g., IS NULL, NOT IN (0,1))
    
    # Define the core SQL condition based on the test type.
    if test_type == "not_null":
        base_condition_content = f'{quoted_column_name} IS NULL'
    elif test_type == "unique":
        # For unique, the base condition typically ensures the column is not null,
        # as duplicates of NULLs are often not considered violations by themselves.
        base_condition_content = f'{quoted_column_name} IS NOT NULL'
    elif test_type == "custom_sql":
        sql_condition = config.get("sql")
        if not sql_condition: 
            logging.error(f"Custom SQL test for {model_name}.{column_name} is missing 'sql' configuration.")
            return None # Custom SQL tests require a 'sql' key in config
        base_condition_content = sql_condition
    else:
        logging.warning(f"Unknown test type '{test_type}'. Cannot generate SQL.")
        return None 

    # --- Construct the WHERE clause components ---
    all_where_conditions = []
    if base_condition_content:
        # Wrap the base condition in parentheses for logical grouping.
        all_where_conditions.append(f'({base_condition_content})') 

    # Add time window filter if specified in the test configuration.
    date_window_col = config.get('date_window_col')
    check_yesterday_only = config.get('check_yesterday_only', False)

    time_window_filter_content = ""
    if date_window_col:
        quoted_date_window_col = f'"{date_window_col}"'
        if check_yesterday_only:
            # Filter for data from yesterday based on the date_window_col.
            time_window_filter_content = f"{quoted_date_window_col} >= CURRENT_DATE - INTERVAL '1 DAY' AND {quoted_date_window_col} < CURRENT_DATE"
            logging.debug(f"  -> Applying 'yesterday only' filter: {time_window_filter_content}")
        else:
            # Filter for data within a specified number of recent days.
            time_window_days = config.get('time_window_days')
            if isinstance(time_window_days, int) and time_window_days > 0:
                time_window_filter_content = f"{quoted_date_window_col} >= CURRENT_DATE - INTERVAL '{time_window_days} DAY'"
                logging.debug(f"  -> Applying time window filter: {time_window_filter_content}")
        
        if time_window_filter_content:
            all_where_conditions.append(f'({time_window_filter_content})') 

    # Combine all WHERE conditions with 'AND'.
    final_where_clause = ""
    if all_where_conditions:
        final_where_clause = " WHERE " + " AND ".join(all_where_conditions)

    # --- Construct the final SQL query based on test type ---
    if test_type == "unique":
        # For 'unique' tests, we need a subquery to find duplicates.
        # The WHERE clause applies to the inner selection before grouping.
        return f"""
        SELECT COUNT(*) FROM (
            SELECT {quoted_column_name}
            FROM {quoted_model_name}
            {final_where_clause}
            GROUP BY {quoted_column_name}
            HAVING COUNT(*) > 1
        )
        """
    else: 
        # For 'not_null', 'custom_sql', and other simple count queries,
        # directly apply the WHERE clause to count failing rows.
        return f'SELECT COUNT(*) FROM {quoted_model_name}{final_where_clause}'


def run_all_checks(conn: snowflake.connector.SnowflakeConnection, rules: list) -> list:
    """
    Executes all generated data quality checks against Snowflake.

    Args:
        conn: An active Snowflake connection object.
        rules: A list of dictionaries, where each dictionary represents a 'model'
               with a 'name' and a list of 'tests' to execute.

    Returns:
        list: A list of dictionaries, each representing the result of a single test,
              including status, failing rows, and metadata.
    """
    cursor = conn.cursor()
    results = []
    # Calculate total number of tests to be run for progress tracking.
    total_tests = sum(len(model.get('tests', [])) for model in rules)
    if total_tests == 0:
        logging.warning("No tests were generated. Exiting.")
        return []

    logging.info(f"--- Starting Execution of {total_tests} Automatic DQ Tests ---")
    
    test_counter = 0
    # Iterate through each model (table) and its associated tests.
    for model in rules:
        model_name = model['name']
        for test in model.get('tests', []):
            test_counter += 1
            test_type = test.get('type')
            column = test.get('column_name', 'N/A')
            description = test.get('description', 'No description.')
            config = test.get('config')
            
            logging.info(f"({test_counter}/{total_tests}) Running '{test_type}' on {model_name}.{column}")
            
            # Generate the SQL query for the current test.
            sql = generate_test_sql(test_type, model_name, column, config)

            if not sql:
                # If SQL generation fails, log an error and mark the test as 'error'.
                logging.warning(f"  -> Skipping test due to invalid SQL generation or unknown type: {test_type}")
                status = "error"
                failing_rows = -1 # Use -1 to indicate an execution issue, not 0 failing rows.
            else:
                try:
                    # Execute the SQL query with a defined timeout.
                    cursor.execute(sql, timeout=QUERY_TIMEOUT_SECONDS)
                    failing_rows = cursor.fetchone()[0] # The query returns a single count.
                    status = "fail" if failing_rows > 0 else "pass" # Determine status based on failing rows count.
                except snowflake.connector.errors.ProgrammingError as e:
                    # Handle specific Snowflake errors like query timeout or other SQL errors.
                    if "timed out" in str(e).lower() or e.errno == 604: # Snowflake timeout error code is 604
                        logging.warning(f"  -> Test TIMED OUT for {model_name}.{column}")
                        status = "timeout"
                    else:
                        logging.error(f"  -> Test ERROR for {model_name}.{column}: {e}")
                        status = "error"
                    failing_rows = -1 # Indicate error/timeout with -1 failing rows.
                except Exception as e:
                    # Catch any other unexpected Python errors during execution.
                    logging.error(f"  -> UNEXPECTED ERROR for {model_name}.{column}: {e}")
                    status = "error"
                    failing_rows = -1
            
            # Append the test result to the results list.
            results.append({
                "model_name": model_name, "column_name": column, "test_type": test_type,
                "status": status, "failing_rows": int(failing_rows), "description": description,
                "timestamp": datetime.utcnow().isoformat() # Record the UTC timestamp of the test run.
            })

    cursor.close()
    return results

# --- Main Execution Flow ---
def main():
    """
    Main function to orchestrate the entire automatic data quality check process.
    It connects to Snowflake, discovers tables, generates rules, runs tests,
    and saves the results.
    """
    start_time = time.perf_counter() # Record start time for performance measurement.
    logging.info("======================================================")
    logging.info("  Starting Automatic Data Quality Check Orchestrator  ")
    logging.info("======================================================")
    
    # Establish Snowflake connection. Exit if connection fails.
    conn = get_snowflake_connection()
    if not conn:
        sys.exit(1)

    # Flag to enable 'yesterday only' checks across all generated automatic rules.
    # This is useful for daily incremental checks.
    check_yesterday_only = True 

    results = [] # Initialize results list
    try:
        # Step 1: Discover active tables and their primary date columns.
        active_tables_info = discover_active_tables(conn)
        
        # Step 2: Generate data quality rules for the active tables.
        # The 'check_yesterday_only' flag is propagated here.
        rules = generate_all_rules(conn, active_tables_info, check_yesterday_only)
        
        # Step 3: Run all generated data quality checks.
        results = run_all_checks(conn, rules)
    finally:
        # Ensure the Snowflake connection is always closed.
        if conn:
            conn.close()
            logging.info("Snowflake connection closed.")
    
    # --- Results Reporting and Summary ---
    if results:
        # Save the results to a JSON file for dashboard consumption.
        with open(RESULTS_FILE, 'w') as f:
            json.dump(results, f, indent=2)
        logging.info(f"✅ Data quality checks complete. Results saved to {RESULTS_FILE}")
    else:
        logging.warning("No results were generated.")
        # Create an empty results file so the dashboard doesn't break if no tests run.
        with open(RESULTS_FILE, 'w') as f:
            json.dump([], f)
    
    # Calculate and log summary statistics of test results.
    passed = sum(1 for r in results if r['status'] == 'pass')
    failed = sum(1 for r in results if r['status'] == 'fail')
    errors = sum(1 for r in results if r['status'] == 'error')
    timeouts = sum(1 for r in results if r['status'] == 'timeout')
    
    logging.info("--- Summary ---")
    logging.info(f"Total Tests Run: {len(results)}")
    logging.info(f"✅ Passed: {passed}")
    logging.info(f"❌ Failed: {failed}")
    logging.info(f"⚠️ Errors: {errors}")
    logging.info(f"⏱️ Timed Out: {timeouts}")
    
    total_elapsed = time.perf_counter() - start_time # Calculate total execution time.
    logging.info(f"Total execution time: {total_elapsed:.2f} seconds")
    logging.info("======================================================")
    
    # Exit with a non-zero code if there were any failures, errors, or timeouts.
    # This is crucial for CI/CD integration to signal build failures.
    if failed > 0 or errors > 0 or timeouts > 0:
        sys.exit(1)

# --- Script Entry Point ---
if __name__ == "__main__":
    main()