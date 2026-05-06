"""load/load.py – Stage Layer Loader (Full Refresh).

Strategy:
    1. Connect to SQL Server
    2. Audit: get current min/max date + row count from stage
    3. Validate incoming DataFrame (not empty, no duplicates, required cols)
    4. TRUNCATE + INSERT inside a single transaction
       → If INSERT fails, TRUNCATE is rolled back (stage stays intact)
    5. Log result

Why one transaction?
    TRUNCATE then INSERT separately means: if INSERT crashes,
    stage is empty → MERGE into Bronze will be skipped silently.
    Wrapping both in BEGIN TRAN / COMMIT makes it atomic.
"""

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine, Connection
from typing import Optional, Tuple
from datetime import datetime, timezone

from utils.config_loader import DBConfig
from utils.logger import get_logger

logger = get_logger("load.stage")

# ── Table references (must match ddl.sql) ─────────────────────
STAGE_SCHEMA = "stage"
STAGE_TABLE  = "stock_stage"
FULL_TABLE   = f"{STAGE_SCHEMA}.{STAGE_TABLE}"

# Columns that must exist in the DataFrame
REQUIRED_COLS = ["trade_date", "ticker", "open_price", "high_price", "low_price", "close_price", "volume"]


# ============================================================
# Main entry point — called by pipeline.py
# ============================================================

def load_to_stage(df: pd.DataFrame, db: DBConfig) -> int:
    """
    Full-refresh load of validated DataFrame into stage.stage_layer.

    Steps:
        1. Build & test DB engine
        2. Audit current state of stage layer
        3. Validate incoming DataFrame
        4. Atomic: TRUNCATE → INSERT (single transaction)

    Args:
        df  : Cleaned DataFrame from transformation/clean.py
        db  : DBConfig (SQL Server credentials from .env)

    Returns:
        Number of rows loaded into stage

    Raises:
        ValueError   : df is empty or has bad data
        RuntimeError : DB connection or insert failure
    """
    logger.info("=" * 55)
    logger.info("STAGE LAYER — Full Refresh Load")
    logger.info("=" * 55)

    # Step 1 — Connect ─────────────────────────────────────────
    engine = _get_engine(db)

    # Step 2 — Audit current state ─────────────────────────────
    min_d, max_d, current_rows = _get_stage_info(engine)
    if current_rows > 0:
        logger.info(f"Stage BEFORE load  → {current_rows} rows | {min_d} → {max_d}")
    else:
        logger.info("Stage is currently empty (first run or already cleared)")

    # Step 3 — Validate incoming data ──────────────────────────
    _validate_dataframe(df)
    logger.info(f"Incoming data      → {len(df)} rows | "
                f"{df['trade_date'].min()} → {df['trade_date'].max()} | "
                f"tickers: {sorted(df['ticker'].unique().tolist())}")

    # Step 4 — Prepare df (drop loaded_at if present; SQL Server fills via DEFAULT)
    df_load = _prepare_for_insert(df)

    # Step 5 — Atomic TRUNCATE + INSERT ────────────────────────
    rows_loaded = _truncate_and_insert(df_load, engine)

    logger.info(f"Stage AFTER load   → {rows_loaded} rows loaded ✓")
    engine.dispose()
    return rows_loaded


# ============================================================
# Private helpers
# ============================================================

def _get_engine(db: DBConfig) -> Engine:
    """Build SQLAlchemy engine and verify the connection."""
    try:
        engine = create_engine(
            db.get_sqlalchemy_url(),
            fast_executemany=True,    # pyodbc bulk insert optimization
        )
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info(f"Connected: {db.server} / {db.database}")
        return engine
    except Exception as e:
        logger.error(f"DB connection failed: {e}")
        raise RuntimeError(f"DB connection failed: {e}") from e


def _get_stage_info(engine: Engine) -> Tuple[Optional[str], Optional[str], int]:
    """
    Read current min date, max date, and row count from stage layer.
    Returns (None, None, 0) if table is empty or doesn't exist yet.
    """
    query = f"""
        SELECT
            CONVERT(VARCHAR(10), MIN(trade_date), 120) AS min_date,
            CONVERT(VARCHAR(10), MAX(trade_date), 120) AS max_date,
            COUNT(*)                             AS row_count
        FROM {FULL_TABLE}
    """
    try:
        with engine.connect() as conn:
            row = conn.execute(text(query)).fetchone()
        return (
            str(row[0]) if row[0] else None,
            str(row[1]) if row[1] else None,
            int(row[2]) if row[2] else 0,
        )
    except Exception as e:
        logger.warning(f"Could not query stage (first run?): {e}")
        return None, None, 0


def _validate_dataframe(df: pd.DataFrame) -> None:
    """
    Validate the DataFrame before loading.

    Checks:
      - Not None / not empty
      - All required columns present
      - No null Date or Ticker
      - No duplicate (Date, Ticker) — PK constraint in DDL would reject them
    """
    if df is None or df.empty:
        raise ValueError("DataFrame is empty — aborting load.")

    missing_cols = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    null_dates   = df["trade_date"].isna().sum()
    null_tickers = df["ticker"].isna().sum()
    if null_dates or null_tickers:
        raise ValueError(
            f"Data has {null_dates} null dates and {null_tickers} null tickers — aborting."
        )

    # Check for duplicates — stage PK is (trade_date, ticker)
    dups = df.duplicated(subset=["trade_date", "ticker"], keep=False)
    if dups.sum():
        dup_examples = df[dups][["trade_date", "ticker"]].head(5).to_string(index=False)
        raise ValueError(
            f"DataFrame has {dups.sum()} duplicate (trade_date, ticker) rows.\n"
            f"Examples:\n{dup_examples}\n"
            f"Run deduplication in clean.py before loading."
        )

    logger.info("DataFrame validation passed ✓")


def _prepare_for_insert(df: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare DataFrame for SQL Server insert.

    - Keep only the columns defined in the stage table
    - 'loaded_at' is NOT included → SQL Server fills it via DEFAULT GETUTCDATE()
    - Ensure correct dtypes match DDL column types
    """
    df = df[REQUIRED_COLS].copy()

    # Match DDL dtypes precisely
    df["trade_date"]   = pd.to_datetime(df["trade_date"])           # DATE in SQL Server
    df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
    df["open_price"]   = df["open_price"].astype(float).round(4)
    df["high_price"]   = df["high_price"].astype(float).round(4)
    df["low_price"]    = df["low_price"].astype(float).round(4)
    df["close_price"]  = df["close_price"].astype(float).round(4)
    df["volume"] = df["volume"].astype("int64")

    return df


def _truncate_and_insert(df: pd.DataFrame, engine: Engine) -> int:
    """
    TRUNCATE stage layer then INSERT rows — both in a single transaction.

    If INSERT fails → transaction is rolled back → stage keeps old data.
    This protects against the "empty stage" scenario after a crash.

    Note: SQL Server TRUNCATE *can* be rolled back when inside an
          explicit transaction (unlike some other databases).
    """
    logger.info(f"Starting atomic TRUNCATE + INSERT for {len(df)} rows...")

    try:
        with engine.begin() as conn:                    # auto COMMIT on exit, ROLLBACK on error
            # 4a — TRUNCATE (inside transaction → rollback-safe)
            conn.execute(text(f"TRUNCATE TABLE {FULL_TABLE}"))
            logger.info(f"  TRUNCATE {FULL_TABLE} ✓")

            # 4b — INSERT via pandas to_sql (append = table already truncated)
            df.to_sql(
                name      = STAGE_TABLE,
                con       = conn,                       # reuse same connection/transaction
                schema    = STAGE_SCHEMA,
                if_exists = "append",
                index     = False,
                chunksize = 1000,
                method    = "multi",
            )
            logger.info(f"  INSERT {len(df)} rows ✓")

        return len(df)

    except Exception as e:
        # engine.begin() automatically rolls back on exception
        logger.error(f"TRUNCATE+INSERT failed — transaction rolled back: {e}")
        raise RuntimeError(f"Stage load failed (rolled back): {e}") from e
