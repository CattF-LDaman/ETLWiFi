from __future__ import annotations

import polars as pl

WINDOW_TIMESTAMP_COLUMN = "window_timestamp"
PARTITION_DATE_PATTERN = r"^\d{4}-\d{2}-\d{2}$"


def clean_window_timestamp_expr(column: str = WINDOW_TIMESTAMP_COLUMN) -> pl.Expr:
    return pl.col(column).str.replace_all("\x00", "").str.strip_chars()


def partition_date_expr(column: str = WINDOW_TIMESTAMP_COLUMN) -> pl.Expr:
    return clean_window_timestamp_expr(column).str.slice(0, 10)
