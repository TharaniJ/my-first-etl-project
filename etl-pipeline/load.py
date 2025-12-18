# load.py
def run_load(cursor):
    ddl = """
    DROP TABLE IF EXISTS rpt_txn_val_by_currency;
    CREATE TABLE rpt_txn_val_by_currency (
      txn_date        VARCHAR(10)  NOT NULL,   -- DD-MM-YYYY
      currency_code   VARCHAR(10)  NOT NULL,
      total_txn_value BIGINT       NOT NULL,
      txn_count       BIGINT       NOT NULL,
      PRIMARY KEY (txn_date, currency_code)
    );
    """

    for stmt in [s.strip() for s in ddl.split(";") if s.strip()]:
        cursor.execute(stmt)

    insert_sql = """
    INSERT INTO rpt_txn_val_by_currency (txn_date, currency_code, total_txn_value, txn_count)
    SELECT
      txn_date,
      currency_code,
      SUM(amount_rounded) AS total_txn_value,
      COUNT(*)            AS txn_count
    FROM stg_account_transaction
    GROUP BY txn_date, currency_code;
    """
    cursor.execute(insert_sql)

    cursor.execute("SELECT COUNT(*) FROM rpt_txn_val_by_currency;")
    (cnt,) = cursor.fetchone()
    assert cnt >= 0
    return cnt

# Run the load step
def test_load(cursor):
    cnt = run_load(cursor)
    print(f"Loaded {cnt} rows into rpt_txn_val_by_currency")
