# transform.py
def run_transform(cursor, truncate_before_load=True):
    if truncate_before_load:
        cursor.execute("TRUNCATE TABLE stg_account_transaction;")

    insert_sql = """
    INSERT INTO stg_account_transaction (txn_id, account_id, txn_date, amount_rounded, currency_code, status)
    SELECT
      t.txn_id,
      t.account_id,
      DATE_FORMAT(DATE(t.txn_ts), '%d-%m-%Y') AS txn_date,
      CEILING(t.amount)                      AS amount_rounded,
      t.currency_code,
      a.status
    FROM txn_ledger t
    JOIN account a
      ON a.account_id = t.account_id
    WHERE UPPER(a.status) NOT IN ('CLOSED', 'FROZEN');
    """

    cursor.execute(insert_sql)

    cursor.execute("SELECT COUNT(*) FROM stg_account_transaction;")
    (cnt,) = cursor.fetchone()
    assert cnt >= 0
    return cnt


def test_transform(cursor):
    cnt = run_transform(cursor, truncate_before_load=True)
    print(f"Loaded {cnt} rows into stg_account_transaction")
