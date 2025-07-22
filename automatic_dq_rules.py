# automatic_dq_rules.py
import logging
from typing import List, Dict, Any
from datetime import datetime

# --- Enhanced Rule Definitions for Your Specific Schema ---
# Updated to match the patterns found in DWH.PUBLIC tables

def generate_rules_for_table(table_name: str, columns: List[Dict[str, Any]], primary_date_column: str, check_yesterday_only: bool = False) -> Dict[str, Any]:
    """
    Generates a list of DQ tests for a given table based on column names and types.
    Enhanced for DWH.PUBLIC schema patterns.

    Args:
        table_name: The fully-qualified name of the table (e.g., "DWH.PUBLIC.ACCOUNTS").
        columns: A list of column dictionaries from a DESCRIBE TABLE command.
                 Each dict should have 'name' and 'type' keys (from Snowflake DESCRIBE).
        primary_date_column: The best date/timestamp column identified for the table's activity check.
        check_yesterday_only: If True, all generated tests will filter for data from yesterday
                              using the primary_date_column.

    Returns:
        A dictionary representing the "model" and its auto-generated "tests".
    """
    tests = []
    logging.info(f"Generating automatic rules for table: {table_name}")

    # Helper function to check if a column type is numeric
    def is_numeric_type(col_type: str) -> bool:
        return 'NUMBER' in col_type.upper() or 'FLOAT' in col_type.upper()

    # Helper function to check if a column type is boolean-like
    def is_boolean_like_type(col_type: str) -> bool:
        return 'BOOLEAN' in col_type.upper() or col_type.upper() == 'NUMBER(1,0)'

    # Helper function to check if a column is a variant/JSON type
    def is_variant_type(col_type: str) -> bool:
        return 'VARIANT' in col_type.upper()

    # Base config for every test
    base_test_config = {
        'date_window_col': primary_date_column,
        'check_yesterday_only': check_yesterday_only
    }
    
    # Adjust description prefix based on the check mode
    description_prefix_base = "Auto-generated"
    if check_yesterday_only and primary_date_column:
        description_prefix_base += f" (on {primary_date_column} for yesterday only)"
    elif primary_date_column:
        description_prefix_base += f" (on {primary_date_column} for recent data)"

    for col in columns:
        col_name = col['name']
        col_type = col['type']
        col_name_upper = col_name.upper()

        # Create a copy of the base config for each test
        current_test_config = base_test_config.copy()
        description_prefix = description_prefix_base

        # --- Rule 1: Enhanced ID Checks ---
        # Primary keys should be not null and unique
        if col_name_upper in ['ID', 'USER_ID', 'DATE_ID', 'CAMPAIGN_ID']:
            tests.append({
                'type': 'not_null',
                'column_name': col_name,
                'description': f"{description_prefix}: Primary key '{col_name}' should not be NULL.",
                'config': current_test_config.copy() 
            })
            tests.append({
                'type': 'unique',
                'column_name': col_name,
                'description': f"{description_prefix}: Primary key '{col_name}' should be unique.",
                'config': current_test_config.copy()
            })
            logging.debug(f"  -> Applied 'not_null' and 'unique' rules to primary key '{col_name}'")

        # Foreign key IDs should be not null (but not necessarily unique)
        elif col_name_upper.endswith('_ID') or col_name_upper in ['UTM_KEY']:
            tests.append({
                'type': 'not_null',
                'column_name': col_name,
                'description': f"{description_prefix}: Foreign key '{col_name}' should not be NULL.",
                'config': current_test_config.copy() 
            })
            logging.debug(f"  -> Applied 'not_null' rule to foreign key '{col_name}'")

        # --- Rule 2: Timestamp/Date Columns ---
        timestamp_columns = ['CREATEDAT', 'UPDATEDAT', 'DATE', 'INSTALL_DATE', 'FIRST_ACTIVE', 
                           'LAST_ACTIVE', 'SUB_START_DATE', 'FIRST_IMPRESSION_DATE', 
                           'LAST_IMPRESSION_DATE', 'FIRST_BUFF_PLAY_ACTIVITY']
        if col_name_upper in timestamp_columns:
            tests.append({
                'type': 'not_null',
                'column_name': col_name,
                'description': f"{description_prefix}: Timestamp column '{col_name}' should not be NULL.",
                'config': current_test_config.copy()
            })
            
            # Check for future dates (except for projection tables like DIM_DATES)
            if 'DIM_DATES' not in table_name.upper():
                current_date = datetime.now().strftime('%Y-%m-%d')
                sql_condition = f'"{col_name}" > CURRENT_DATE() AND "{col_name}" IS NOT NULL'
                tests.append({
                    'type': 'custom_sql',
                    'column_name': col_name,
                    'config': {**current_test_config.copy(), 'sql': sql_condition},
                    'description': f"{description_prefix}: Timestamp '{col_name}' should not be in the future."
                })
            logging.debug(f"  -> Applied timestamp rules to column '{col_name}'")

        # --- Rule 3: Email Format Check ---
        if col_name_upper == 'EMAIL':
            tests.append({
                'type': 'not_null',
                'column_name': col_name,
                'description': f"{description_prefix}: Email column '{col_name}' should not be NULL.",
                'config': current_test_config.copy()
            })
            # Enhanced email regex for better validation
            sql_condition = f'NOT (REGEXP_LIKE("{col_name}", \'[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{{2,}}\') OR "{col_name}" IS NULL)'
            tests.append({
                'type': 'custom_sql',
                'column_name': col_name,
                'config': {**current_test_config.copy(), 'sql': sql_condition},
                'description': f"{description_prefix}: Email column '{col_name}' should have a valid format."
            })
            logging.debug(f"  -> Applied email validation rules to column '{col_name}'")

        # --- Rule 4: Country Code Checks ---
        if col_name_upper in ['COUNTRY', 'COUNTRY_CODE']:
            # Check for reasonable country code length (2-3 chars typically)
            sql_len_condition = f'(LEN("{col_name}") < 2 OR LEN("{col_name}") > 3) AND "{col_name}" IS NOT NULL AND "{col_name}" != \'\''
            tests.append({
                'type': 'custom_sql',
                'column_name': col_name,
                'config': {**current_test_config.copy(), 'sql': sql_len_condition},
                'description': f"{description_prefix}: Country code '{col_name}' should be 2-3 characters."
            })
            
            # Check for empty strings or just spaces (common data quality issue in your schema)
            sql_empty_condition = f'TRIM("{col_name}") = \'\' AND "{col_name}" IS NOT NULL'
            tests.append({
                'type': 'custom_sql',
                'column_name': col_name,
                'config': {**current_test_config.copy(), 'sql': sql_empty_condition},
                'description': f"{description_prefix}: Country code '{col_name}' should not be empty or whitespace."
            })
            logging.debug(f"  -> Applied country code rules to column '{col_name}'")

        # --- Rule 5: Financial/Metric Columns (Enhanced for your schema) ---
        financial_keywords = ['BALANCE', 'BONUSBALANCE', 'SPEND', 'REVENUE', 'LTV', 'DAILY_SPEND_USD', 
                             'DAILY_SPEND_ILS', 'DAILY_SPEND_ARS', 'POINTS']
        if is_numeric_type(col_type) and any(k in col_name_upper for k in financial_keywords):
            # Most financial metrics should be non-negative
            if 'REFUND' not in col_name_upper and 'ADJUSTMENT' not in col_name_upper:
                sql_condition = f'"{col_name}" < 0 AND "{col_name}" IS NOT NULL'
                tests.append({
                    'type': 'custom_sql',
                    'column_name': col_name,
                    'config': {**current_test_config.copy(), 'sql': sql_condition},
                    'description': f"{description_prefix}: Financial column '{col_name}' should be non-negative."
                })
                logging.debug(f"  -> Applied non-negative rule to financial column '{col_name}'")

        # --- Rule 6: Campaign/Marketing Metrics ---
        campaign_keywords = ['IMPRESSIONS', 'CLICKS', 'INSTALLS', 'CAMPAIGN_DURATION']
        if is_numeric_type(col_type) and any(k in col_name_upper for k in campaign_keywords):
            # These should always be non-negative
            sql_condition = f'"{col_name}" < 0 AND "{col_name}" IS NOT NULL'
            tests.append({
                'type': 'custom_sql',
                'column_name': col_name,
                'config': {**current_test_config.copy(), 'sql': sql_condition},
                'description': f"{description_prefix}: Campaign metric '{col_name}' should be non-negative."
            })
            
            # Reasonable upper bounds for some metrics
            if col_name_upper == 'CAMPAIGN_DURATION':
                sql_condition = f'"{col_name}" > 730 AND "{col_name}" IS NOT NULL'  # More than 2 years seems excessive
                tests.append({
                    'type': 'custom_sql',
                    'column_name': col_name,
                    'config': {**current_test_config.copy(), 'sql': sql_condition},
                    'description': f"{description_prefix}: Campaign duration '{col_name}' exceeds reasonable limit (730 days)."
                })
            logging.debug(f"  -> Applied campaign metric rules to column '{col_name}'")

        # --- Rule 7: Boolean/Indicator Flags (Enhanced) ---
        boolean_indicators = ['IS_SUB_IND', 'IS_ACTIVE_SUB_IND', 'ISCONFIRMEDEMAIL', 'ISFROZEN', 
                            'ISCLOSED', 'IS_REWARDED', 'IS_PREMIUM', 'CURRENT_YEAR_IND', 
                            'CURRENT_MONTH_IND', 'REGISTRATION_APP']
        if col_name_upper in boolean_indicators or (is_boolean_like_type(col_type) and any(k in col_name_upper for k in ['IS_', '_IND'])):
            if 'BOOLEAN' in col_type.upper():
                sql_condition = f'"{col_name}" NOT IN (TRUE, FALSE) AND "{col_name}" IS NOT NULL'
            else:
                sql_condition = f'"{col_name}" NOT IN (0, 1) AND "{col_name}" IS NOT NULL'
                
            tests.append({
                'type': 'custom_sql',
                'column_name': col_name,
                'config': {**current_test_config.copy(), 'sql': sql_condition},
                'description': f"{description_prefix}: Boolean indicator '{col_name}' should have valid boolean values."
            })
            logging.debug(f"  -> Applied boolean validation rule to column '{col_name}'")

        # --- Rule 8: Dimensional Table Specific Rules ---
        if is_numeric_type(col_type):
            if col_name_upper == 'MONTH_ID':
                sql_condition = f'("{col_name}" < 1 OR "{col_name}" > 12) AND "{col_name}" IS NOT NULL'
                tests.append({
                    'type': 'custom_sql',
                    'column_name': col_name,
                    'config': {**current_test_config.copy(), 'sql': sql_condition},
                    'description': f"{description_prefix}: MONTH_ID should be between 1 and 12."
                })

            elif col_name_upper == 'QUARTER_ID':
                sql_condition = f'("{col_name}" < 1 OR "{col_name}" > 4) AND "{col_name}" IS NOT NULL'
                tests.append({
                    'type': 'custom_sql',
                    'column_name': col_name,
                    'config': {**current_test_config.copy(), 'sql': sql_condition},
                    'description': f"{description_prefix}: QUARTER_ID should be between 1 and 4."
                })

            elif col_name_upper == 'WEEK_NUMBER':
                sql_condition = f'("{col_name}" < 1 OR "{col_name}" > 53) AND "{col_name}" IS NOT NULL'
                tests.append({
                    'type': 'custom_sql',
                    'column_name': col_name,
                    'config': {**current_test_config.copy(), 'sql': sql_condition},
                    'description': f"{description_prefix}: WEEK_NUMBER should be between 1 and 53."
                })

            elif col_name_upper == 'YEAR':
                current_year = datetime.now().year
                sql_condition = f'("{col_name}" < 2015 OR "{col_name}" > {current_year + 10}) AND "{col_name}" IS NOT NULL'
                tests.append({
                    'type': 'custom_sql',
                    'column_name': col_name,
                    'config': {**current_test_config.copy(), 'sql': sql_condition},
                    'description': f"{description_prefix}: YEAR should be within business-relevant range (2015-{current_year + 10})."
                })

        # --- Rule 9: UTM_KEY and Campaign Validation ---
        if col_name_upper == 'UTM_KEY':
            # UTM_KEY should not be empty strings or just spaces
            sql_condition = f'(TRIM("{col_name}") = \'\' OR "{col_name}" = \' \') AND "{col_name}" IS NOT NULL'
            tests.append({
                'type': 'custom_sql',
                'column_name': col_name,
                'config': {**current_test_config.copy(), 'sql': sql_condition},
                'description': f"{description_prefix}: UTM_KEY should not be empty or whitespace."
            })
            
            # Reasonable length check for UTM_KEY
            sql_condition = f'LEN("{col_name}") > 200 AND "{col_name}" IS NOT NULL'
            tests.append({
                'type': 'custom_sql',
                'column_name': col_name,
                'config': {**current_test_config.copy(), 'sql': sql_condition},
                'description': f"{description_prefix}: UTM_KEY length exceeds reasonable limit (200 chars)."
            })
            logging.debug(f"  -> Applied UTM_KEY validation rules")

        # --- Rule 10: Campaign and Marketing Source Validation ---
        marketing_fields = ['SOURCE', 'MEDIUM', 'CAMPAIGN', 'MARKETING_SOURCE', 'DATA_SOURCE']
        if col_name_upper in marketing_fields:
            # Should not be empty strings or just spaces
            sql_condition = f'(TRIM("{col_name}") = \'\' OR "{col_name}" = \' \') AND "{col_name}" IS NOT NULL'
            tests.append({
                'type': 'custom_sql',
                'column_name': col_name,
                'config': {**current_test_config.copy(), 'sql': sql_condition},
                'description': f"{description_prefix}: Marketing field '{col_name}' should not be empty or whitespace."
            })

        # --- Rule 11: User Type and Role Validation ---
        if col_name_upper in ['USER_TYPE', 'ROLE', 'AUTHTYPE', 'PREM_TYPE']:
            # Should have reasonable values (not empty, not just spaces)
            sql_condition = f'(TRIM("{col_name}") = \'\' OR "{col_name}" = \' \') AND "{col_name}" IS NOT NULL'
            tests.append({
                'type': 'custom_sql',
                'column_name': col_name,
                'config': {**current_test_config.copy(), 'sql': sql_condition},
                'description': f"{description_prefix}: Classification field '{col_name}' should not be empty or whitespace."
            })

        # --- Rule 12: Game ID Validation ---
        game_fields = ['FIRST_GAME', 'LAST_GAME', 'MAIN_GAME']
        if col_name_upper in game_fields and is_numeric_type(col_type):
            # Game IDs should be positive if not null
            sql_condition = f'"{col_name}" <= 0 AND "{col_name}" IS NOT NULL'
            tests.append({
                'type': 'custom_sql',
                'column_name': col_name,
                'config': {**current_test_config.copy(), 'sql': sql_condition},
                'description': f"{description_prefix}: Game ID '{col_name}' should be positive."
            })

        # --- Rule 13: Subscription Logic Validation ---
        if col_name_upper == 'NUMBER_OF_ACTIVE_SUBSCRIPTIONS' and is_numeric_type(col_type):
            # Should not be negative
            sql_condition = f'"{col_name}" < 0 AND "{col_name}" IS NOT NULL'
            tests.append({
                'type': 'custom_sql',
                'column_name': col_name,
                'config': {**current_test_config.copy(), 'sql': sql_condition},
                'description': f"{description_prefix}: NUMBER_OF_ACTIVE_SUBSCRIPTIONS should not be negative."
            })
            
            # Should not exceed reasonable limit
            sql_condition = f'"{col_name}" > 10 AND "{col_name}" IS NOT NULL'
            tests.append({
                'type': 'custom_sql',
                'column_name': col_name,
                'config': {**current_test_config.copy(), 'sql': sql_condition},
                'description': f"{description_prefix}: NUMBER_OF_ACTIVE_SUBSCRIPTIONS exceeds reasonable limit (10)."
            })

        # --- Rule 14: Currency Validation ---
        if col_name_upper == 'CURRENCY':
            # Should be standard 3-letter currency codes
            sql_condition = f'LEN("{col_name}") != 3 AND "{col_name}" IS NOT NULL'
            tests.append({
                'type': 'custom_sql',
                'column_name': col_name,
                'config': {**current_test_config.copy(), 'sql': sql_condition},
                'description': f"{description_prefix}: Currency code should be 3 characters."
            })

        # --- Rule 15: VARIANT/JSON Field Validation ---
        if is_variant_type(col_type):
            # Check that VARIANT fields contain valid JSON when not null
            sql_condition = f'TRY_PARSE_JSON("{col_name}") IS NULL AND "{col_name}" IS NOT NULL AND "{col_name}" != \'null\''
            tests.append({
                'type': 'custom_sql',
                'column_name': col_name,
                'config': {**current_test_config.copy(), 'sql': sql_condition},
                'description': f"{description_prefix}: VARIANT field '{col_name}' should contain valid JSON."
            })

    # ------------------------------------------------------------------ #
    # === NEW CROSS‑COLUMN RULES (from automatic_dq_rules_updated.py) ===#
    # ------------------------------------------------------------------ #
    col_index = {c["name"].upper(): c["name"] for c in columns}

    # 1. DATE_ID ↔ DATE alignment (YYYYMMDD)
    if {"DATE_ID", "DATE"}.issubset(col_index):
        sql = (
            f'TO_NUMBER(TO_CHAR("{col_index["DATE"]}", \'YYYYMMDD\')) <> "{col_index["DATE_ID"]}" '
            f'AND "{col_index["DATE"]}" IS NOT NULL AND "{col_index["DATE_ID"]}" IS NOT NULL'
        )
        tests.append(
            {
                "type": "custom_sql",
                "column_name": col_index["DATE_ID"],
                "config": {**base_test_config, "sql": sql},
                "description": f"{description_prefix_base}: DATE_ID must equal YYYYMMDD representation of DATE.",
            }
        )

    # 2. createdAt should not be after updatedAt
    if {"CREATEDAT", "UPDATEDAT"}.issubset(col_index):
        sql = (
            f'"{col_index["CREATEDAT"]}" > "{col_index["UPDATEDAT"]}" '
            f'AND "{col_index["CREATEDAT"]}" IS NOT NULL AND "{col_index["UPDATEDAT"]}" IS NOT NULL'
        )
        tests.append(
            {
                "type": "custom_sql",
                "column_name": col_index["CREATEDAT"],
                "config": {**base_test_config, "sql": sql},
                "description": f"{description_prefix_base}: createdAt must be ≤ updatedAt.",
            }
        )

    # 3. FIRST_IMPRESSION_DATE ≤ LAST_IMPRESSION_DATE
    if {"FIRST_IMPRESSION_DATE", "LAST_IMPRESSION_DATE"}.issubset(col_index):
        sql = (
            f'"{col_index["FIRST_IMPRESSION_DATE"]}" > "{col_index["LAST_IMPRESSION_DATE"]}" '
            f'AND "{col_index["FIRST_IMPRESSION_DATE"]}" IS NOT NULL AND "{col_index["LAST_IMPRESSION_DATE"]}" IS NOT NULL'
        )
        tests.append(
            {
                "type": "custom_sql",
                "column_name": col_index["FIRST_IMPRESSION_DATE"],
                "config": {**base_test_config, "sql": sql},
                "description": f"{description_prefix_base}: FIRST_IMPRESSION_DATE must not be after LAST_IMPRESSION_DATE.",
            }
        )

    # 4. DATE should lie between FIRST_IMPRESSION_DATE and LAST_IMPRESSION_DATE
    if {"DATE", "FIRST_IMPRESSION_DATE", "LAST_IMPRESSION_DATE"}.issubset(col_index):
        sql = (
            f'("{col_index["DATE"]}" < "{col_index["FIRST_IMPRESSION_DATE"]}" '
            f'OR "{col_index["DATE"]}" > "{col_index["LAST_IMPRESSION_DATE"]}") '
            f'AND "{col_index["DATE"]}" IS NOT NULL'
        )
        tests.append(
            {
                "type": "custom_sql",
                "column_name": col_index["DATE"],
                "config": {**base_test_config, "sql": sql},
                "description": f"{description_prefix_base}: DATE must be between FIRST_IMPRESSION_DATE and LAST_IMPRESSION_DATE.",
            }
        )

    # 5. CLICKS should never exceed IMPRESSIONS
    if {"CLICKS", "IMPRESSIONS"}.issubset(col_index):
        sql = (
            f'"{col_index["CLICKS"]}" > "{col_index["IMPRESSIONS"]}" '
            f'AND "{col_index["CLICKS"]}" IS NOT NULL AND "{col_index["IMPRESSIONS"]}" IS NOT NULL'
        )
        tests.append(
            {
                "type": "custom_sql",
                "column_name": col_index["CLICKS"],
                "config": {**base_test_config, "sql": sql},
                "description": f"{description_prefix_base}: CLICKS cannot exceed IMPRESSIONS.",
            }
        )

    # --- Rule 16: Table-Specific Cross-Column Validation (Existing in original) ---
    # Add business logic rules based on table patterns
    if 'ACCOUNTS' in table_name.upper():
        # If email is confirmed, email should not be null
        if any(col['name'].upper() == 'ISCONFIRMEDEMAIL' for col in columns) and any(col['name'].upper() == 'EMAIL' for col in columns):
            sql_condition = 'ISCONFIRMEDEMAIL = TRUE AND EMAIL IS NULL'
            tests.append({
                'type': 'custom_sql',
                'column_name': 'isConfirmedEmail,email',
                'config': {**base_test_config.copy(), 'sql': sql_condition},
                'description': f"{description_prefix_base}: If email is confirmed, email field should not be null."
            })

    elif 'DIM_USERS' in table_name.upper():
        # If user has active subscription, should have subscription indicator
        if any(col['name'].upper() == 'IS_ACTIVE_SUB_IND' for col in columns) and any(col['name'].upper() == 'NUMBER_OF_ACTIVE_SUBSCRIPTIONS' for col in columns):
            sql_condition = 'IS_ACTIVE_SUB_IND = 1 AND (NUMBER_OF_ACTIVE_SUBSCRIPTIONS IS NULL OR NUMBER_OF_ACTIVE_SUBSCRIPTIONS = 0)'
            tests.append({
                'type': 'custom_sql',
                'column_name': 'IS_ACTIVE_SUB_IND,NUMBER_OF_ACTIVE_SUBSCRIPTIONS',
                'config': {**base_test_config.copy(), 'sql': sql_condition},
                'description': f"{description_prefix_base}: Active subscription indicator should match subscription count."
            })

    return {
        'name': table_name,
        'tests': tests
    }