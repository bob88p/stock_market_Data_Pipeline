from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
import numpy as np
from datetime import datetime
import pandas as pd
import os
from sqlalchemy import create_engine, text

from utils.config_loader import load_config
from extract.reader import read_all
from transformation.clean import clean as clean_df
from transformation.check import check as check_df
from load.load import load_to_stage
from load.merge import merge_stage_to_bronze

config = load_config("/opt/airflow/config/job_config.yaml")



#extract → clean → check_clean → load_stage → check_stage → merge → cleanup



# -------------------------------
# Helpers
# -------------------------------

def get_last_loaded_date(engine):
    query = """
        SELECT MAX(trade_date)
        FROM bronze.stock_prices_raw
    """

    with engine.connect() as conn:
        result = conn.execute(text(query))
        return result.scalar()


def get_paths():
    context = get_current_context()
    ds = context["ds"]

    base_path = f"/tmp/medallion_pipeline/{ds}"
    os.makedirs(base_path, exist_ok=True)

    return {
        "base": base_path,
        "raw": f"{base_path}/raw.parquet",
        "valid": f"{base_path}/valid.parquet",
        "silver": f"{base_path}/silver.parquet",
    }
# -----------------------
# DAG
# -----------------------

@dag(
    dag_id="medallion_stock_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,   # manual trigger
    catchup=False,
    tags=["etl", "medallion"]
)
def medallion_pipeline():

    # -----------------------
    # Extract
    # -----------------------
    @task
    def extract():
        paths = get_paths()

        engine = create_engine(config.db.connection_string)

        last_date = get_last_loaded_date(engine)

        df = read_all(
            symbols=config.input.symbols,
            period=config.input.period,
            interval=config.input.interval,
            auto_adjust=config.input.auto_adjust
        )

        if df.empty:
            raise ValueError("No data extracted")

        # ---------------------------
        # Incremental Logic
        # ---------------------------

        if last_date is not None:
            df["trade_date"] = pd.to_datetime(df["trade_date"])
            df = df[df["trade_date"] > last_date]
            if df.empty:
                raise ValueError("No new data after incremental filter")

        df.to_parquet(paths["raw"], index=False)


    # -----------------------
    # Clean
    # -----------------------
    @task
    def clean():
        paths = get_paths()

        df = pd.read_parquet(paths["raw"])
        valid, rejected = clean_df(df)

        if valid.empty:
            raise ValueError("No valid data")

        valid.to_parquet(paths["valid"], index=False)

    # -----------------------
    # Check
    # -----------------------
    @task
    def check():
        paths = get_paths()

        df = pd.read_parquet(paths["valid"])

        report = check_df(
            df,
            stage_name="Silver Clean Data",
            expected_cols=["trade_date", "ticker", "open_price", "high_price", "low_price", "close_price", "volume"]
        )

        if not report.is_valid:
            raise ValueError("Quality check failed")


    # -----------------------
    # Load Stage
    # -----------------------
    @task
    def load_stage():
        paths = get_paths()

        df = pd.read_parquet(paths["valid"])

        rows = load_to_stage(df, config.db)

        if rows != len(df):
            raise ValueError("Stage mismatch")



    #------------
    #check 

    @task
    def check_stage():
            paths = get_paths()
            engine = create_engine(config.db.connection_string)

            # 1. Row count
            df = pd.read_parquet(paths["valid"])
            file_count = len(df)

            db_count = pd.read_sql(
                "SELECT COUNT(*) FROM stage.stock_stage",
                engine
            ).iloc[0, 0]

            if file_count != db_count:
                raise ValueError("Row count mismatch")

            # 2. Duplicates
            dup = pd.read_sql("""
                SELECT ticker, trade_date, COUNT(*) cnt
                FROM stage.stock_stage
                GROUP BY ticker, trade_date
                HAVING COUNT(*) > 1
            """, engine)

            if not dup.empty:
                raise ValueError("Duplicates found")

            # 3. Integrity
            invalid = pd.read_sql("""
                SELECT *
                FROM stage.stock_stage
                WHERE high_price < low_price
                OR close_price > high_price
                OR close_price < low_price
            """, engine)

            if not invalid.empty:
                raise ValueError("Integrity check failed")

            # 4. Nulls
            nulls = pd.read_sql("""
                SELECT *
                FROM stage.stock_stage
                WHERE trade_date IS NULL
                OR ticker IS NULL
                OR close_price IS NULL
            """, engine)

            if not nulls.empty:
                raise ValueError("Null values found")


    # -----------------------
    # Merge Bronze
    # -----------------------
    @task
    def merge():
        result = merge_stage_to_bronze(config.db)

        if result.rows_inserted + result.rows_updated == 0:
            raise ValueError("No merge happened")




    @task
    def build_silver():
        paths = get_paths()
        engine = create_engine(config.db.connection_string)

        last_date_query = "SELECT MAX(trade_date) FROM silver.stock_prices_clean"
        try:
            last_date = pd.read_sql(last_date_query, engine).iloc[0, 0]
        except:
            last_date = None

        # -------------------------
        # query
        # -------------------------
        if last_date is not None:
            query = text("""
            WITH context AS (
                SELECT * FROM (
                    SELECT *,
                        ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY trade_date DESC) rn
                    FROM bronze.stock_prices_raw
                    WHERE trade_date <= :last_date
                ) t WHERE rn <= 7
            ),
            new_data AS (
                SELECT *
                FROM bronze.stock_prices_raw
                WHERE trade_date > :last_date
            )
            SELECT * FROM context
            UNION ALL
            SELECT * FROM new_data
            """)

            df = pd.read_sql(query, engine, params={"last_date": last_date})
        else:
            df = pd.read_sql("SELECT * FROM bronze.stock_prices_raw", engine)

        if df.empty:
            raise ValueError("No data for silver")

        df["trade_date"] = pd.to_datetime(df["trade_date"])
        df = df.sort_values(["ticker", "trade_date"])

        g = df.groupby("ticker", group_keys=False)

        df["prev_close"] = g["close_price"].shift(1)

        df["daily_return_pct"] = np.where(
            df["prev_close"] == 0,
            np.nan,
            (df["close_price"] - df["prev_close"]) / df["prev_close"]
        )

        df["price_range"] = df["high_price"] - df["low_price"]

        df["ma_7"] = g["close_price"].rolling(7).mean().reset_index(level=0, drop=True)

        df["volatility_7d"] = (
            g["daily_return_pct"]
            .rolling(7)
            .std()
            .reset_index(level=0, drop=True)
        )

        # keep only new rows
        if last_date is not None:
            df = df[df["trade_date"] > pd.to_datetime(last_date)]

        # Drop helper col
        if "prev_close" in df.columns:
            df = df.drop(columns=["prev_close"])

        # -------------------------
        #  save parquet
        # -------------------------
        df.to_parquet(paths["silver"], index=False)

        return paths["silver"]



    @task
    def load_silver(parquet_path):
        engine = create_engine(config.db.connection_string)

        df = pd.read_parquet(parquet_path)

        if df.empty:
            return

        min_date = df["trade_date"].min()

        with engine.begin() as conn:
            conn.execute(
                text("DELETE FROM silver.stock_prices_clean WHERE trade_date >= :min_date"),
                {"min_date": min_date}
            )

        df.to_sql(
            "stock_prices_clean",
            engine,
            schema="silver",
            if_exists="append",
            index=False
        )



    @task
    def refresh_gold():
        engine = create_engine(config.db.connection_string)

        query = """
        CREATE OR ALTER VIEW gold.fact_stock_prices_daily AS
        SELECT
            trade_date,
            ticker,
            open_price,
            close_price,
            daily_return_pct,
            price_range,
            ma_7,
            volatility_7d
        FROM silver.stock_prices_clean
        """

        with engine.begin() as conn:
            conn.execute(text(query))



    @task
    def cleanup_task():
        paths = get_paths()

        for file in ["raw", "valid", "silver"]:
            if os.path.exists(paths[file]):
                os.remove(paths[file])

        # remove folder
        if os.path.exists(paths["base"]):
            os.rmdir(paths["base"])


    # -----------------------
    # Dependencies (IMPORTANT)
    # -----------------------
    e = extract()
    c = clean()
    ch = check()
    ls = load_stage()
    cs = check_stage()
    m = merge()

    silver_path = build_silver()
    s1 = load_silver(silver_path)
    g = refresh_gold()

    cl = cleanup_task()

    e >> c >> ch >> ls >> cs >> m >> silver_path >> s1 >> g >> cl
# Create DAG instance
medallion_pipeline()



#extract >> clean >> check_clean >> load_stage >> merge >> cleanup