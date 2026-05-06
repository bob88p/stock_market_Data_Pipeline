"""transformation/check.py – Data Quality & Validation Utility.

This module provides reusable validation functions to check data integrity 
at different stages of the ETL pipeline:
    1. Post-Extraction (Clean check)
    2. Post-Loading to Stage
    3. Post-Merging to Bronze
"""

import pandas as pd
import numpy as np
from datetime import datetime
from typing import List, Optional, Dict, Any
from dataclasses import dataclass, field

from utils.logger import get_logger

logger = get_logger("transformation.check")

@dataclass
class QualityReport:
    stage_name: str
    is_valid: bool = True
    errors: List[str] = field(default_factory=list)
    metrics: Dict[str, Any] = field(default_factory=dict)

def check(
    df: pd.DataFrame, 
    stage_name: str,
    expected_cols: List[str] = None,
    pk_cols: List[str] = ["trade_date", "ticker"],
    allow_nulls: bool = False,
    date_col: str = "trade_date"
) -> QualityReport:
    """
    Perform a suite of data quality checks on a DataFrame.
    
    Args:
        df: The DataFrame to check.
        stage_name: Name of the ETL stage (e.g., 'Extraction', 'Stage Layer', 'Bronze Layer').
        expected_cols: List of columns that must exist.
        pk_cols: Columns that define a unique record (for duplicate check).
        allow_nulls: If False, any NULL in the dataframe triggers an error.
        date_col: The name of the date column for period checks.
        
    Returns:
        QualityReport object containing the results.
    """
    report = QualityReport(stage_name=stage_name)
    logger.info(f"--- Data Quality Check: {stage_name} ---")
    
    if df is None or df.empty:
        msg = f"[{stage_name}] DataFrame is empty or None."
        logger.error(msg)
        report.is_valid = False
        report.errors.append(msg)
        return report

    # 1. Schema Check (Required Columns)
    if expected_cols:
        missing_cols = [col for col in expected_cols if col not in df.columns]
        if missing_cols:
            msg = f"[{stage_name}] Missing required columns: {missing_cols}"
            logger.error(msg)
            report.is_valid = False
            report.errors.append(msg)
    
    # 2. Null Check
    if not allow_nulls:
        null_counts = df.isnull().sum()
        cols_with_nulls = null_counts[null_counts > 0]
        if not cols_with_nulls.empty:
            msg = f"[{stage_name}] Found NULL values in columns: {cols_with_nulls.to_dict()}"
            logger.error(msg)
            report.is_valid = False
            report.errors.append(msg)
        report.metrics["null_counts"] = null_counts.to_dict()

    # 3. Duplicate Check
    if all(col in df.columns for col in pk_cols):
        duplicates = df.duplicated(subset=pk_cols).sum()
        if duplicates > 0:
            msg = f"[{stage_name}] Found {duplicates} duplicate records based on {pk_cols}."
            logger.error(msg)
            report.is_valid = False
            report.errors.append(msg)
        report.metrics["duplicate_count"] = int(duplicates)

    # 4. Date Period Check
    if date_col in df.columns:
        try:
            temp_dates = pd.to_datetime(df[date_col])
            min_date = temp_dates.min()
            max_date = temp_dates.max()
            now = datetime.now()
            
            report.metrics["min_date"] = str(min_date.date())
            report.metrics["max_date"] = str(max_date.date())
            
            if max_date > now:
                msg = f"[{stage_name}] Found future dates: max date is {max_date.date()}"
                logger.warning(msg)
                # Not necessarily failing validity unless strictly required
            
            logger.info(f"[{stage_name}] Date range: {min_date.date()} to {max_date.date()}")
        except Exception as e:
            msg = f"[{stage_name}] Failed to validate dates: {e}"
            logger.error(msg)
            report.is_valid = False
            report.errors.append(msg)

    # Summary
    if report.is_valid:
        logger.info(f"[{stage_name}] Quality check PASSED. Rows: {len(df)}")
    else:
        logger.error(f"[{stage_name}] Quality check FAILED with {len(report.errors)} errors.")
        
    return report
