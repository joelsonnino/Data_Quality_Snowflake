# data_quality/explore_table.py

import os
import sys
import pandas as pd
from dotenv import load_dotenv
import snowflake.connector

# --- Configuration & Setup ---
# Determine the project root directory to correctly locate the .env file.
# This makes the script runnable from various subdirectories if needed.
project_root = os.path.abspath(os.path.dirname(__file__))
dotenv_path = os.path.join(project_root, '.env')

# Check if the .env file exists and load environment variables.
# The .env file is crucial for storing sensitive Snowflake credentials.
if not os.path.exists(dotenv_path):
    print(f"Error: .env file not found at {dotenv_path}")
    print("Please ensure the .env file with your Snowflake credentials exists in the project root.")
    sys.exit(1) # Exit if credentials file is missing to prevent runtime errors.

load_dotenv(dotenv_path)

# --- Snowflake Connection Management ---
def get_snowflake_connection():
    """
    Establishes a connection to Snowflake using credentials loaded from the .env file.

    Returns:
        snowflake.connector.connection.SnowflakeConnection: An active Snowflake connection object,
                                                             or None if the connection fails.
    """
    try:
        # Attempt to connect using environment variables for user, password, account, role, and warehouse.
        conn = snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            role=os.getenv("SNOWFLAKE_ROLE"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE")
        )
        print("✅ Successfully connected to Snowflake.")
        return conn
    except Exception as e:
        # Catch any exceptions during connection and print an error message.
        print(f"❌ Could not connect to Snowflake. Aborting. Error: {e}")
        return None

# --- Core Table Exploration Logic ---
def explore_table():
    """
    Prompts the user for a fully-qualified Snowflake table name,
    fetches and displays the first 3 rows of data, along with
    suggestions for potential data quality controls.

    This function is designed to assist data engineers and analysts
    in understanding table structure and data patterns for DQ rule creation.
    """
    print("--- Snowflake Table Explorer ---")
    # Prompt for the full table name (e.g., DATABASE.SCHEMA.TABLE).
    full_table_name = input("Enter the fully-qualified table name (e.g., DATABASE.SCHEMA.TABLE): ")

    # Validate the input format to ensure it's a fully qualified name.
    if not full_table_name or len(full_table_name.split('.')) != 3:
        print("❌ Invalid format. Please use the format: DATABASE.SCHEMA.TABLE")
        return

    # Split the fully qualified name and wrap each part in double quotes.
    # Double quotes are crucial in Snowflake to handle case-sensitivity and
    # identifiers that might contain special characters or be reserved words.
    db, schema, table = [f'"{p}"' for p in full_table_name.split('.')]
    
    # Construct the SQL query to fetch a small sample of data.
    query = f"SELECT * FROM {db}.{schema}.{table} LIMIT 3"
    
    # Establish a Snowflake connection. Abort if connection fails.
    conn = get_snowflake_connection()
    if not conn:
        return

    try:
        print(f"\n🔍 Fetching first 3 rows from {full_table_name}...")
        
        # Use pandas' read_sql function to directly execute the query and
        # load the results into a DataFrame, simplifying data retrieval.
        df = pd.read_sql(query, conn)
        
        # Check if the DataFrame is empty, indicating an empty table.
        if df.empty:
            print(f"\n⚠️ The table '{full_table_name}' exists but appears to be empty.")
            return

        print(f"\n--- First 3 Rows of {full_table_name} ---\n")
        # Use .to_string() to print the entire DataFrame without truncation,
        # ensuring all columns and rows are visible in the console.
        print(df.to_string())

        # Provide actionable suggestions for data quality rule creation
        # based on common data quality dimensions.
        print("\n\n--- 🤔 Potential Controls to Implement ---\n")
        print("Based on this sample, consider these data quality checks:")
        print("  - `not_null`: Are there any columns that should NEVER be empty (e.g., ID columns)?")
        print("  - `unique`: Should values in a specific column be unique (e.g., primary keys like 'ACCOUNT_ID')?")
        print("  - `accepted_values`: For columns like 'STATUS' or 'TYPE', should they only contain a specific list of values (e.g., ['active', 'inactive'])?")
        print("  - `relationship`: Should an ID in this table (e.g., 'USER_ID') exist in another table (e.g., DIM_USERS)?")
        print("  - `custom_sql`: Do you have specific business rules? (e.g., 'IF a user is inactive, their balance MUST be 0').")
        print("\nUse this insight to create or update a test in your `dq_rules/` YAML files.")

    except snowflake.connector.errors.ProgrammingError as e:
        # Catch specific Snowflake programming errors (e.g., table not found, syntax errors, permissions).
        print(f"\n❌ An error occurred while executing the query: {e}")
        print("   Please check if the table name is correct and if you have the necessary permissions.")
    except Exception as e:
        # Catch any other unexpected errors.
        print(f"\n❌ An unexpected error occurred: {e}")
    finally:
        # Ensure the Snowflake connection is closed regardless of success or failure.
        if conn:
            conn.close()
            print("\n✅ Snowflake connection closed.")

# --- Script Entry Point ---
if __name__ == "__main__":
    # Configure pandas display options for optimal console output.
    # 'display.max_columns=None' prevents column truncation.
    # 'display.width=200' sets the maximum display width, useful for wide tables.
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 200)
    
    # Execute the main exploration function.
    explore_table()