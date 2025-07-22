# Project Documentation: Snowflake Data Quality Control Platform

## Table of Contents

1.  [Introduction](#1-introduction)
2.  [Problem Solved & Context](#2-problem-solved--context)
3.  [Project Goals](#3-project-goals)
4.  [System Architecture](#4-system-architecture)
5.  [Key Components](#5-key-components)
    *   [run_automatic_dq_checks.py](#run_automatic_dq_checkspy)
    *   [automatic_dq_rules.py](#automatic_dq_rulespy)
    *   [dq_dashboard.py](#dq_dashboardpy)
    *   [discover_schemas_filtered.py](#discover_schemas_filteredpy)
    *   [explore_table.py](#explore_tablepy)
    *   [utils.py](#utilspy)
    *   [config.py](#configpy)
    *   [.env](#env)
    *   [requirements.txt](#requirementstxt)
    *   [dq_reports/](#dq_reports)
    *   [dq_rules/](#dq_rules)
6.  [Project Setup](#6-project-setup)
7.  [How to Run DQ Checks](#7-how-to-run-dq-checks)
    *   [Running Automatic DQ Checks](#running-automatic-dq-checks)
    *   [Interactive Table Exploration](#interactive-table-exploration)
    *   [Running the Dashboard](#running-the-dashboard)
8.  [Configuration Guide](#8-configuration-guide)
    *   [Target Schemas Configuration (`config.py`)](#target-schemas-configuration-configpy)
    *   [Customizing Automatic Rules (`automatic_dq_rules.py`)](#customizing-automatic-rules-automatic_dq_rulespy)
    *   [Table Whitelisting (`discover_schemas_filtered.py`)](#table-whitelisting-discover_schemas_filteredpy)
    *   [Query Timeout (`run_automatic_dq_checks.py`)](#query-timeout-run_automatic_dq_checkspy)
9.  [Extensibility & Future Enhancements](#9-extensibility--future-enhancements)
10. [Troubleshooting](#10-troubleshooting)
11. [Conclusion](#11-conclusion)

---

## 1. Introduction

This document provides a comprehensive guide to the **Snowflake Data Quality Control Platform**, a rule-based system designed to monitor and ensure data integrity and quality within the Snowflake data warehouse. Developed in the context of a gaming company, the system focuses on verifying domain-specific data (e.g., game events, user profiles, session statistics) through a hybrid approach:

*   **Automatic, Convention-Based Checks:** An intelligent system that generates data quality tests for all active tables, inferring rules based on column naming conventions and data types. This approach drastically reduces manual configuration.
*   **Manual Rules (YAML-based):** While not fully integrated into the current primary execution flow, the platform is designed to support the definition of more complex and specific business rules via YAML files.

The platform aims to be the essential complement to any Anomaly Detection system, focusing on the proactive and continuous application of predefined data integrity rules.

## 2. Problem Solved & Context

In today's data-driven business landscape, especially within the high-volume, high-velocity gaming industry, data reliability is paramount. Poor data quality can lead to:

*   **Flawed Business Decisions:** Based on inaccurate or incomplete metrics.
*   **Operational Issues:** Difficulties in analyzing user behavior, optimizing marketing campaigns, or enhancing the gaming experience.
*   **Loss of Trust:** Analysts and stakeholders lose confidence in the data, reducing the adoption of data-driven solutions.

The manual data quality control process is often inefficient and unscalable. This project addresses the need to automate, standardize, and monitor data quality at scale, ensuring that data pipelines deliver accurate and reliable information for strategic analysis and decision-making.

## 3. Project Goals

The primary objectives of this platform are:

*   **Automation & Standardization:** Execute DQ checks automatically and consistently across all relevant tables.
*   **Broad Coverage:** Proactively monitor a large number of tables and columns without the need for extensive individual configurations.
*   **Scalable Maintainability:** Facilitate the easy addition of new rules and the seamless integration of new tables.
*   **Intuitive Monitoring:** Provide a centralized dashboard to visualize data health status and quickly identify issues.
*   **Flexibility:** Allow for the definition of custom business rules (via SQL) when automatic conventions are insufficient.
*   **Active Table Identification:** Focus checks only on tables receiving recent updates, optimizing resource utilization.

## 4. System Architecture

The system is designed with a clear separation of concerns to ensure modularity, scalability, and maintainability.

**Data Flow and Processes:**

1.  **Snowflake Connection:** The `utils.py` module manages secure connections to Snowflake using credentials defined in the `.env` file.
2.  **Active Table Discovery:** The `run_automatic_dq_checks.py` script (internally, via functions like `discover_active_tables`) scans the target schemas defined in `config.py`. It identifies "active" tables (those with data updated within the last 30 days) using an automatically detected date column.
3.  **Automatic Rule Generation:** For each discovered active table, `run_automatic_dq_checks.py` invokes `automatic_dq_rules.py`. This module contains the convention-based logic that generates a specific set of DQ tests (e.g., `not_null`, `unique`, `custom_sql`) for the table's columns, based on their names and types.
4.  **Test Execution:** `run_automatic_dq_checks.py` executes the SQL queries generated for each test. Each test is a `SELECT COUNT(*)` query designed to identify the number of rows that fail the defined rule.
5.  **Results Collection and Storage:** The results of each test (status, number of failing rows, timestamp, etc.) are collected and written to the `dq_reports/dq_results.json` file.
6.  **Reporting and Visualization:** The Streamlit dashboard (`dq_dashboard.py`) reads `dq_results.json` to provide an interactive visualization of data health status, trends, and critical issues.
7.  **Support Tools:**
    *   `explore_table.py`: An interactive command-line tool for exploring the schema and sample data of a single table, useful during rule development and debugging.
    *   `discover_schemas_filtered.py`: A more comprehensive discovery script that outputs a readable catalog of filtered tables.

**Simplified Flow Diagram:**

```
+----------------+       +-------------------+       +---------------------+
|    Snowflake   | ----> | 1. Table Discovery| ----> | 2. Rule Generation  |
| (Data Source)  |       |   (run_automatic) |       | (automatic_dq_rules)|
+----------------+       +-------------------+       +----------+----------+
                                                      |          |
                                                      v          |
+---------------------+      +-----------------+      +----------+
| 5. DQ Results (JSON)| <----| 4. Test Execution| <----| 3. Generated Rules  |
|  (dq_reports)       |      |(run_automatic)  |      |                      |
+---------------------+      +-----------------+      +----------+----------+
          ^
          |
+---------------------+
| 6. DQ Dashboard     |
|   (dq_dashboard)    |
+---------------------+
```

## 5. Key Components

This section describes the function of each primary file and folder within the project.

### `run_automatic_dq_checks.py`
The **main orchestrator script**. It serves as the entry point for executing automatic DQ checks.
*   Manages the end-to-end flow: from connecting to Snowflake, discovering active tables, generating rules, executing tests, and writing results.
*   Leverages `utils`, `config`, and `automatic_dq_rules` modules.
*   Handles query execution errors (timeouts, SQL errors).
*   Generates a final summary of test runs to the console.

### `automatic_dq_rules.py`
The **heart of the automatic rule intelligence**.
*   Contains the `generate_rules_for_table` function which, given a table and its columns, generates a list of DQ tests based on conventions (e.g., `ID` not null and unique, `EMAIL` with valid format, non-negative financial columns).
*   Allows for the definition of single-column rules and cross-column rules.
*   This is the file to modify when adding new data quality conventions.

### `dq_dashboard.py`
The **interactive dashboard** for visualizing results.
*   Built using Streamlit.
*   Reads the `dq_results.json` file to display key metrics (health score, pass/fail counts), distribution charts, and a detailed table of results.
*   Offers interactive filters (by status, table, test type, date range) and data download options.
*   Includes custom CSS styling for enhanced user experience.

### `discover_schemas_filtered.py`
A **utility tool for schema discovery and catalog analysis**.
*   Scans specific schemas in Snowflake.
*   Identifies "active" tables (those with recent data) and/or whitelisted tables.
*   Generates a text report (`schema_discovery_filtered_output.txt`) including each table's schema definition and a sample of data (first 3 rows).
*   Produces a JSON log (`skipped_tables_log.json`) of tables that were skipped and the reason.
*   **Note:** Although it performs a "discovery" function, its output is not directly consumed by the `run_automatic_dq_checks.py` process, which has its own lighter internal discovery logic. It is primarily a manual analysis tool.

### `explore_table.py`
An **interactive command-line tool** for single table exploration.
*   Prompts the user for a fully-qualified Snowflake table name.
*   Connects to Snowflake and displays the first 3 rows of the specified table.
*   Provides suggestions for potential DQ checks that could be applied to that table. Useful for debugging and rule prototyping.

### `utils.py`
A **shared utility module**.
*   Contains the `get_snowflake_connection()` function, which handles establishing a Snowflake connection, reusable by all scripts requiring database access.
*   Manages loading environment variables from the `.env` file.

### `config.py`
The **global configuration file**.
*   Defines `TARGET_SCHEMAS`, which are the Snowflake database schemas (e.g., `("DWH", "PUBLIC")`) that the data quality system should scan.

### `.env`
A **file for sensitive credentials**.
*   **Must NOT be committed to version control!** Contains Snowflake access credentials (user, password, account, role, warehouse).
*   An `.env.example` file (if present) shows the expected format.

### `requirements.txt`
The **project's dependency manifest**.
*   Lists all required Python libraries for the platform to run (e.g., `snowflake-connector-python`, `pandas`, `streamlit`, `python-dotenv`, `pyyaml`).
*   Used to install all dependencies via `pip install -r requirements.txt`.

### `dq_reports/`
A **directory for report outputs**.
*   `dq_results.json`: The JSON file generated by `run_automatic_dq_checks.py` containing the detailed results of all executed DQ tests. This is the file read by the dashboard.
*   `schema_discovery_filtered_output.txt`: The human-readable output of `discover_schemas_filtered.py`.
*   `skipped_tables_log.json`: A JSON log of tables excluded from the discovery process, generated by `discover_schemas_filtered.py`.

### `dq_rules/`
A **directory for manual (YAML) DQ rules**.
*   Contains YAML files (e.g., `dwh_public.yml`, `product_pg_s3.yml`) where users can define custom, business-specific DQ tests.
*   **Important Note:** In the context of the provided code, these YAML files are present but are not currently read and executed by the main flow (`run_automatic_dq_checks.py`). The platform is designed to extend to support the execution of these manual rules, but this would require further implementation of loading and executing tests from these files.

## 6. Project Setup

Follow these steps to set up your environment and get the project running.

1.  **Clone the Repository (if you haven't already):**
    ```bash
    git clone <YOUR_REPOSITORY_URL>
    cd <project_folder_name>
    ```

2.  **Create the `.env` File:**
    *   In the root directory of your project, create a file named `.env`.
    *   Populate it with your Snowflake credentials. **Do not commit this file to version control.**
    ```env
    # .env
    SNOWFLAKE_USER="your_snowflake_user"
    SNOWFLAKE_PASSWORD="your_snowflake_password"
    SNOWFLAKE_ACCOUNT="your_snowflake_account_identifier" # e.g., xyz12345.eu-central-1
    SNOWFLAKE_ROLE="your_snowflake_role"    # e.g., ANALYST_ROLE
    SNOWFLAKE_WAREHOUSE="your_snowflake_warehouse" # e.g., COMPUTE_WH
    ```
    *   Ensure that the role and warehouse have the necessary permissions to describe and read tables in your target schemas.

3.  **Install Python Dependencies:**
    *   It is highly recommended to use a virtual environment (`venv`).
    ```bash
    python -m venv venv
    source venv/bin/activate # On Linux/macOS
    # or: .\venv\Scripts\activate # On Windows PowerShell
    ```
    *   Install the required libraries using `requirements.txt`:
    ```bash
    pip install -r requirements.txt
    ```

## 7. How to Run DQ Checks

### Running Automatic DQ Checks

This is the primary method for executing convention-based data quality checks.

```bash
python run_automatic_dq_checks.py
```

*   This script will connect to Snowflake, discover active tables in the schemas defined in `config.py`, generate DQ tests for them, and execute them.
*   Results will be saved to `dq_reports/dq_results.json`.
*   A summary of the execution will be printed to the console. The script will exit with a non-zero error code (`sys.exit(1)`) if there were any failed tests, execution errors, or timeouts, which is useful for CI/CD pipelines.

### Interactive Table Exploration

Use this script to quickly inspect the schema and sample data of a single table. Useful for debugging or understanding data before defining rules.

```bash
python explore_table.py
```

*   You will be prompted to enter the fully-qualified table name (e.g., `DATABASE.SCHEMA.TABLE`).
*   The first 3 rows of the table and suggestions for potential DQ controls will be displayed.

### Running the Dashboard

To visualize the DQ check results in an interactive graphical interface:

```bash
streamlit run dq_dashboard.py
```

*   Ensure you have run `run_automatic_dq_checks.py` at least once to generate the `dq_results.json` file, otherwise the dashboard will appear empty.
*   The command will open a web browser tab with the dashboard.

## 8. Configuration Guide

The system is designed to be highly configurable with minimal effort.

### Target Schemas Configuration (`config.py`)

To define which Snowflake database schemas should be scanned by the automatic DQ checks:

1.  Open `config.py`.
2.  Modify the `TARGET_SCHEMAS` list by adding or removing `(database, schema)` tuples.
    ```python
    # config.py
    TARGET_SCHEMAS = [
        ("DWH", "PUBLIC"),
        ("PRODUCT_PG", "S3"),
        # Add more schemas here
        ("YOUR_DB", "YOUR_SCHEMA"),
    ]
    ```

### Customizing Automatic Rules (`automatic_dq_rules.py`)

This is where you define your organization's data quality conventions.

1.  Open `automatic_dq_rules.py`.
2.  Within the `generate_rules_for_table` function, you can add new rules or modify existing ones.
    *   **Example: Add a rule for `STATUS` columns to check accepted values:**
        ```python
        # automatic_dq_rules.py
        # ... (inside the generate_rules_for_table function) ...
        if col_name_upper == 'STATUS':
            tests.append({
                'type': 'custom_sql', # Or 'accepted_values' if you implement that type directly
                'column_name': col_name,
                'config': {
                    **current_test_config.copy(),
                    'sql': f'"{col_name}" NOT IN (\'ACTIVE\', \'INACTIVE\', \'PENDING\') AND "{col_name}" IS NOT NULL'
                },
                'description': f"{description_prefix}: Column '{col_name}' must be one of ['ACTIVE', 'INACTIVE', 'PENDING']."
            })
            logging.debug(f"  -> Applied 'accepted_values' rule to column '{col_name}'")

        # Example: Add a new alias for a timestamp column (e.g., if 'AUDIT_TS' is also used)
        # Modify the 'timestamp_columns' list:
        timestamp_columns = ['CREATEDAT', 'UPDATEDAT', 'DATE', 'INSTALL_DATE', 'FIRST_ACTIVE',
                           'LAST_ACTIVE', 'SUB_START_DATE', 'FIRST_IMPRESSION_DATE',
                           'LAST_IMPRESSION_DATE', 'FIRST_BUFF_PLAY_ACTIVITY', 'EVENT_TIMESTAMP', 'AUDIT_TS'] # <-- Added
        ```
    *   You can also modify the time window behavior by adjusting the `check_yesterday_only` flag or adding `time_window_days` to `base_test_config` for specific test types if you need more granular control beyond the global setting.

### Table Whitelisting (`discover_schemas_filtered.py`)

To ensure specific tables are always included in the discovery process (and thus in DQ analysis), even if they haven't been updated recently or lack a detectable date column:

1.  Open `discover_schemas_filtered.py`.
2.  Modify the `WHITELISTED_TABLES` set by adding fully-qualified table names (e.g., `DATABASE.SCHEMA.TABLE`).
    ```python
    # discover_schemas_filtered.py
    WHITELISTED_TABLES = {
        "PRODUCT_PG.S3.buff_play_service_games",
        "PRODUCT_PG.S3.contest_challenge_contest_challenges_parquet",
        # Add other critical tables here
        "MY_DB.MY_SCHEMA.MY_STATIC_REFERENCE_DATA"
    }
    ```

### Query Timeout (`run_automatic_dq_checks.py`)

To adjust the maximum time allowed for an individual DQ check query to execute in Snowflake:

1.  Open `run_automatic_dq_checks.py`.
2.  Modify the value of `QUERY_TIMEOUT_SECONDS`. Increase it for complex queries on very large tables, or decrease it to fail faster.
    ```python
    # run_automatic_dq_checks.py
    QUERY_TIMEOUT_SECONDS = 60 # e.g., Increased to 60 seconds
    ```

## 9. Extensibility & Future Enhancements

The current platform provides a strong foundation and can be extended in several ways:

*   **Integration of Manual YAML Rules:** Implement a parser and execution logic within `run_automatic_dq_checks.py` to read tests from `dq_rules/*.yml` files and include them in the test execution alongside the automatically generated rules. This would provide the full hybrid capability described.
*   **Notification System:** Add a module for sending alerts (e.g., via email, Slack, PagerDuty) when critical DQ tests fail.
*   **Threshold-Based Alerts:** Enhance `run_all_checks` to allow tests to have configurable failure thresholds (e.g., `max_failing_rows: 100` or `max_failing_percentage: 0.01`). A test would only "fail" if `failing_rows` exceeds this threshold, providing more nuanced alerts.
*   **Historical Trend Analysis in Dashboard:** Store `dq_results.json` with a timestamp in its filename (e.g., `dq_results_2023-10-27.json`) and allow the dashboard to load and visualize historical trends of DQ scores and specific test failures.
*   **Data Lineage Integration:** If a data lineage solution is in place, link DQ failures to upstream data sources or transformation steps.
*   **More Advanced Rule Types:** Implement specialized test types beyond `not_null`, `unique`, `custom_sql`, such as `row_count_anomaly` (comparing current row count to historical patterns), `schema_drift` (detecting unexpected column changes), or `referential_integrity` (checking foreign key relationships).
*   **Configuration Externalization:** For `automatic_dq_rules.py`, consider externalizing the lists of keywords (e.g., `timestamp_columns`, `financial_keywords`) into a separate JSON or YAML file. This would allow data quality analysts to influence automatic rule generation without touching Python code.
*   **Test Case Prioritization:** Implement a mechanism to prioritize certain tests or tables, potentially running critical checks more frequently or with different timeouts.

## 10. Troubleshooting

Here are some common issues and their solutions:

*   **"Error: .env file not found"**:
    *   **Solution:** Ensure you have created the `.env` file in the root directory of the project and populated it with your Snowflake credentials. Double-check the file name (must be exactly `.env`).
*   **"Could not connect to Snowflake"**:
    *   **Solution:** Verify your Snowflake credentials in the `.env` file. Check for typos in user, password, account, role, or warehouse. Ensure your network allows connection to Snowflake.
*   **"An error occurred while executing the query: ... Table does not exist" or "Insufficient privileges"**:
    *   **Solution:** Check the table name you entered (for `explore_table.py`) or the schema names in `config.py`. Ensure the Snowflake role configured in `.env` has `USAGE` privilege on the database and schema, and `SELECT` privilege on the tables you are trying to query.
*   **Dashboard shows "No Data Available"**:
    *   **Solution:** You need to run `python run_automatic_dq_checks.py` first to generate the `dq_results.json` file in the `dq_reports/` directory.
*   **Tests are timing out (status 'timeout')**:
    *   **Solution:** Increase `QUERY_TIMEOUT_SECONDS` in `run_automatic_dq_checks.py`. This might be necessary for queries on very large tables or complex custom SQL rules. Alternatively, optimize the SQL in your `automatic_dq_rules.py` or consider partitioning strategies in Snowflake.
*   **`ModuleNotFoundError`**:
    *   **Solution:** Ensure you have activated your Python virtual environment (`source venv/bin/activate`) and installed all dependencies from `requirements.txt` (`pip install -r requirements.txt`).

## 11. Conclusion

The Snowflake Data Quality Control Platform provides a robust and intelligent solution for managing data quality within a Snowflake data warehouse. By combining automated, convention-based rule generation with a clear architecture and an intuitive dashboard, it significantly enhances data reliability and reduces manual overhead. Its modular design and extensibility make it a valuable asset for any data-driven organization looking to maintain high-quality data at scale.
