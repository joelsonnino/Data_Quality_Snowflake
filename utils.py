# utils.py
"""
This module provides common utility functions for the data quality system,
primarily focusing on establishing and managing Snowflake connections.
"""

import os
import sys
import snowflake.connector
from dotenv import load_dotenv
import logging
from typing import Optional

# Initialize logger for this module.
logger = logging.getLogger(__name__)

# Load environment variables once for the entire application lifecycle.
# This ensures that all modules can access credentials and configurations
# from the .env file without explicitly loading it in each file.
load_dotenv()

def get_snowflake_connection() -> Optional[snowflake.connector.SnowflakeConnection]:
    """
    Creates and returns a connection to Snowflake using credentials retrieved
    from environment variables (loaded from the .env file).

    Returns:
        Optional[snowflake.connector.SnowflakeConnection]: An active Snowflake connection object
                                                           if successful, None otherwise.
    """
    try:
        # Attempt to establish a connection to Snowflake.
        # Credentials (user, password, account, role, warehouse) are fetched
        # from environment variables, which should be set via the .env file.
        conn = snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            role=os.getenv("SNOWFLAKE_ROLE"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE")
        )
        logger.info("Successfully connected to Snowflake.")
        return conn
    except Exception as e:
        # Log a critical error if the connection fails, as most operations
        # depend on a successful database connection.
        logger.critical(f"Could not connect to Snowflake. Aborting. Error: {e}")
        return None