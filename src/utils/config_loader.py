"""utils/config_loader.py – Parses job_config.yaml + .env into typed dataclasses.

Source  : Yahoo Finance via yfinance (no API key needed)
Target  : SQL Server (Bronze → Silver → Gold)

Usage:
    from src.utils.config_loader import load_config
    cfg = load_config("config/job_config.yaml")
    print(cfg.job.name)
    print(cfg.db.get_sqlalchemy_url())
    print(cfg.input.symbols)
"""

import os
import yaml
from dataclasses import dataclass, field
from typing import List, Optional
from dotenv import load_dotenv

# Load .env file automatically
load_dotenv()


# ============================================================
# Dataclasses — one per YAML section
# ============================================================

@dataclass
class JobMeta:
    name: str
    version: str
    description: str


@dataclass
class InputConfig:
    source_name: str
    input_type: str            # "yfinance" | "csv" | "db"
    symbols: List[str]         # e.g. ["AAPL", "MSFT"]
    period: str                # "1y" | "6mo" | "max" ...
    interval: str              # "1d" | "1h" | "1wk" ...
    auto_adjust: bool          # adjust for splits/dividends
    has_header: bool
    input_schema: dict


@dataclass
class RejectionConfig:
    rejection_path: str
    rejection_type: str
    max_rejection_rate: float


@dataclass
class LayerTarget:
    schema: str
    table: str


@dataclass
class OutputConfig:
    output_type: str           # "sqlserver" | "csv" | "parquet"
    save_mode: str             # "upsert" | "overwrite" | "append"
    partition_cols: List[str] = field(default_factory=list)
    bronze: Optional[LayerTarget] = None
    silver: Optional[LayerTarget] = None
    gold:   Optional[LayerTarget] = None


@dataclass
class ETLConfig:
    rules: List[dict] = field(default_factory=list)


@dataclass
class QualityConfig:
    checks: List[str] = field(default_factory=list)


# ============================================================
# Database Config — from .env (never hardcode credentials)
# ============================================================

@dataclass
class DBConfig:
    server:             str
    port:               int
    database:           str
    username:           str
    password:           str
    driver:             str
    trusted_connection: bool

    def get_sqlalchemy_url(self) -> str:
        """SQLAlchemy URL — uses pymssql (works inside Docker without ODBC driver)."""
        return (
            f"mssql+pymssql://{self.username}:{self.password}"
            f"@{self.server}:{self.port}/{self.database}"
        )

    def get_pyodbc_string(self) -> str:
        """Raw pyodbc string — used in connection.py."""
        if self.trusted_connection:
            return (
                f"DRIVER={{{self.driver}}};"
                f"SERVER={self.server};"
                f"DATABASE={self.database};"
                f"Trusted_Connection=yes;"
            )
        return (
            f"DRIVER={{{self.driver}}};"
            f"SERVER={self.server},{self.port};"
            f"DATABASE={self.database};"
            f"UID={self.username};"
            f"PWD={self.password};"
        )


# ============================================================
# Top-level JobConfig
# ============================================================

@dataclass
class JobConfig:
    job:       JobMeta
    input:     InputConfig
    rejection: RejectionConfig
    output:    OutputConfig
    etl:       ETLConfig
    quality:   QualityConfig
    db:        DBConfig        # injected from .env


# ============================================================
# Private helpers
# ============================================================

def _load_db_from_env() -> DBConfig:
    """Read SQL Server credentials from environment / .env file."""
    return DBConfig(
        server             = os.environ["DB_SERVER"],
        port               = int(os.getenv("DB_PORT", 1433)),
        database           = os.environ["DB_NAME"],
        username           = os.getenv("DB_USER", ""),
        password           = os.getenv("DB_PASSWORD", ""),
        driver             = os.getenv("DB_DRIVER", "ODBC Driver 17 for SQL Server"),
        trusted_connection = os.getenv("DB_TRUSTED_CONNECTION", "no").lower() == "yes",
    )


def _parse_output(raw: dict) -> OutputConfig:
    layers = raw.get("layers", {})
    return OutputConfig(
        output_type    = raw["output_type"],
        save_mode      = raw["save_mode"],
        partition_cols = raw.get("partition_cols", []),
        bronze = LayerTarget(**layers["bronze"]) if "bronze" in layers else None,
        silver = LayerTarget(**layers["silver"]) if "silver" in layers else None,
        gold   = LayerTarget(**layers["gold"])   if "gold"   in layers else None,
    )


# ============================================================
# Public entry point
# ============================================================

def load_config(path: str) -> JobConfig:
    """
    Parse job_config.yaml and inject DB secrets from .env.

    Args:
        path: path to job_config.yaml

    Returns:
        JobConfig dataclass with all settings
    """
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    return JobConfig(
        job       = JobMeta(**raw["job"]),
        input     = InputConfig(**raw["input"]),
        rejection = RejectionConfig(**raw["rejection"]),
        output    = _parse_output(raw["output"]),
        etl       = ETLConfig(rules=raw["etl"]["rules"]),
        quality   = QualityConfig(checks=raw["quality"]["checks"]),
        db        = _load_db_from_env(),
    )
