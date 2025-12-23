"""
Tests derived directly from the SQL validation queries:

# REPORT TABLE VALIDATIONS
Scenario 1 – Validate the staging table exists.
Scenario 2 – Validate no closed and frozen account available in the staging table.
Scenario 3 - Validate the txn_ts only in 'DD-MM-YYYY' date format.
Scenario 4 - Validate amount field is roundup.
Scenario 5 - Validate row count matches between source and staging.


# REPORT TABLE VALIDATIONS
# Scenario 1 – Validate the report table exists.
# Scenario 2 – Validating the aggregation missmatch in the reporting table.
# Scenario 3 – Validate the primary key uniqueness in the reporting table.


"""
import pymysql
import pytest

# STAGING TABLE VALIDATIONS
# Scenario 1 – Validate the staging table exists
def test_staging_table_exists(cursor):
    cursor.execute("""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = DATABASE()
          AND table_name = 'stg_account_transaction';
    """)
    (cnt,) = cursor.fetchone()
    assert cnt == 1, "stg_account_transaction table does not exist"

# Scenario 2 – Validate no closed and frozen account available in the staging table
def test_no_closed_or_frozen_accounts_in_staging(cursor):
    cursor.execute("""
        SELECT COUNT(*)
        FROM stg_account_transaction
        WHERE UPPER(status) IN ('CLOSED', 'FROZEN');
    """)
    (cnt,) = cursor.fetchone()
    assert cnt == 0, "CLOSED or FROZEN accounts found in staging"

# Scenario 3 - Validate the txn_ts only in 'DD-MM-YYYY' date format.
def test_txn_date_format(cursor):
    cursor.execute("""
        SELECT COUNT(*)
        FROM stg_account_transaction
        WHERE txn_date NOT REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$';
    """)
    (cnt,) = cursor.fetchone()
    assert cnt == 0, "Invalid txn_date format found in staging"

# Scenario 4 - Validate amount field is roundup
def test_amount_is_rounded_up(cursor):
    cursor.execute("""
        SELECT COUNT(*)
        FROM txn_ledger t
        JOIN stg_account_transaction s
          ON t.txn_id = s.txn_id
        WHERE s.amount_rounded <> CEILING(t.amount);
    """)
    (cnt,) = cursor.fetchone()
    assert cnt == 0, "amount_rounded does not match CEILING(amount)"

# Scenario 5 - Validate row count matches between source and staging
def test_staging_row_count_matches_source(cursor):
    cursor.execute("""
        SELECT COUNT(*)
        FROM txn_ledger t
        JOIN account a
          ON t.account_id = a.account_id
        WHERE UPPER(a.status) NOT IN ('CLOSED', 'FROZEN');
    """)
    (expected_cnt,) = cursor.fetchone()

    cursor.execute("SELECT COUNT(*) FROM stg_account_transaction;")
    (actual_cnt,) = cursor.fetchone()

    assert actual_cnt == expected_cnt, "Row count mismatch between source and staging"



# REPORT TABLE VALIDATIONS
# Scenario 1 – Validate the report table exists
def test_report_table_exists(cursor):
    cursor.execute("""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = DATABASE()
          AND table_name = 'rpt_txn_val_by_currency';
    """)
    (cnt,) = cursor.fetchone()
    assert cnt == 1, "rpt_txn_val_by_currency table does not exist"

# Scenario 2 – Validating the aggregation missmatch in the reporting table.
def test_report_aggregation_accuracy(cursor):
    cursor.execute("""
        SELECT COUNT(*)
        FROM rpt_txn_val_by_currency r
        JOIN (
            SELECT
              txn_date,
              currency_code,
              SUM(amount_rounded) AS total_txn_value,
              COUNT(*)            AS txn_count
            FROM stg_account_transaction
            GROUP BY txn_date, currency_code
        ) s
        ON r.txn_date = s.txn_date
       AND r.currency_code = s.currency_code
       AND (r.total_txn_value <> s.total_txn_value
            OR r.txn_count <> s.txn_count);
    """)
    (cnt,) = cursor.fetchone()
    assert cnt == 0, "Aggregation mismatch found in report table"

# Scenario 3 – Validate the primary key uniqueness in the reporting table.
def test_report_primary_key_uniqueness(cursor):
    cursor.execute("""
        SELECT COUNT(*)
        FROM (
            SELECT txn_date, currency_code, COUNT(*) c
            FROM rpt_txn_val_by_currency
            GROUP BY txn_date, currency_code
            HAVING c > 1
        ) x;
    """)
    (cnt,) = cursor.fetchone()
    assert cnt == 0, "Duplicate primary keys found in report table"
