# extract.py
def run_extract(cursor):
    ddl = """
    DROP TABLE IF EXISTS stg_account_transaction;
    CREATE TABLE stg_account_transaction (
      txn_id         BIGINT        NOT NULL,
      account_id     BIGINT        NOT NULL,
      txn_date       VARCHAR(10)   NOT NULL,   -- DD-MM-YYYY
      amount_rounded BIGINT        NOT NULL,
      currency_code  VARCHAR(10)   NOT NULL,
      status         VARCHAR(50)   NOT NULL,
      PRIMARY KEY (txn_id)
    );
    """

    # pymysql cursor.execute() runs one statement at a time
    for stmt in [s.strip() for s in ddl.split(";") if s.strip()]:
        cursor.execute(stmt)

    cursor.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = 'stg_account_transaction';")
    (exists,) = cursor.fetchone()
    assert exists == 1, "stg_account_transaction was not created"


def test_extract(cursor):
    run_extract(cursor)
