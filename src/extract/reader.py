"""extract/reader.py – Bronze Extract Layer (yfinance).

Downloads OHLCV data for a list of tickers and returns a clean
flat DataFrame with columns: Date, Ticker, Open, High, Low, Close, Volume.
"""

import pandas as pd
import yfinance as yf

from utils.logger import get_logger

logger = get_logger("extract.reader")

# Default tickers — overridden by job_config.yaml → cfg.input.symbols
DEFAULT_TICKERS = ["AAPL", "MSFT", "AMZN", "GOOGL", "META", "TSLA", "NVDA"]


def read_all(
    symbols: list[str] | None = None,
    period: str = "1y",
    interval: str = "1d",
    auto_adjust: bool = True,
) -> pd.DataFrame:
    """
    Download stock data from Yahoo Finance and return a flat DataFrame.

    Args:
        symbols      : list of ticker symbols (e.g. ["AAPL", "MSFT"])
        period       : yfinance period string ("1y", "6mo", "max", …)
        interval     : bar interval ("1d", "1h", "1wk", …)
        auto_adjust  : adjust OHLC for splits/dividends

    Returns:
        DataFrame with columns: Date, Ticker, Open, High, Low, Close, Volume
    """
    tickers = symbols or DEFAULT_TICKERS

    logger.info("=" * 55)
    logger.info("EXTRACT LAYER — yfinance download")
    logger.info(f"  Tickers  : {tickers}")
    logger.info(f"  Period   : {period}  |  Interval: {interval}")
    logger.info("=" * 55)

    raw = yf.download(
        tickers=tickers,
        period=period,
        interval=interval,
        auto_adjust=auto_adjust,
        progress=False,
    )

    if raw.empty:
        logger.warning("yfinance returned an empty DataFrame — check symbols/period.")
        return pd.DataFrame(columns=["Date", "Ticker", "Open", "High", "Low", "Close", "Volume"])

    # yfinance returns a MultiIndex column when >1 ticker is requested.
    # Stack the ticker level to get a flat table.
    if isinstance(raw.columns, pd.MultiIndex):
        df = (
            raw
            .stack(level=1)           # moves Ticker from column level → row index
            .reset_index()
            .rename(columns={"level_1": "ticker", "Datetime": "trade_date", "Date": "trade_date"})
        )
    else:
        # Single ticker: columns are plain OHLCV names
        df = raw.reset_index().rename(columns={"index": "trade_date", "Datetime": "trade_date", "Date": "trade_date"})
        df["ticker"] = tickers[0]

    # Normalise column names (yfinance sometimes uses lowercase)
    # We want specific names: trade_date, ticker, open_price, high_price, low_price, close_price, volume
    rename_map = {
        "Open": "open_price",
        "High": "high_price",
        "Low": "low_price",
        "Close": "close_price",
        "Volume": "volume"
    }
    
    # Title case standard yfinance columns for mapping
    df.columns = [c.strip().title() if c not in ("trade_date", "ticker") else c for c in df.columns]
    df = df.rename(columns=rename_map)

    # Keep only the required columns in the right order
    keep = ["trade_date", "ticker", "open_price", "high_price", "low_price", "close_price", "volume"]
    df = df[[c for c in keep if c in df.columns]].copy()

    logger.info(f"Extracted {len(df)} rows for {df['ticker'].nunique()} tickers")
    logger.info(f"Date range : {df['trade_date'].min()} → {df['trade_date'].max()}")

    return df
