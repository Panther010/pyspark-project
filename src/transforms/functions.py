"""
Expression functions.
"""
from pyspark.sql import DataFrame, Column, functions as F

EXPRESSION_FUNCTIONS = {
    "multiply": lambda cols: F.try_multiply(cols[0], cols[1]),
    "divide": lambda cols: F.try_divide(cols[0], cols[1]),
    "current_timestamp": lambda cols: F.current_timestamp(),
}

FILTER_OPERATORS = {
        "gt": lambda c, v: F.col(c) > F.lit(v),
        "ge": lambda c, v: F.col(c) >= F.lit(v),
        "lt": lambda c, v: F.col(c) < F.lit(v),
        "le": lambda c, v: F.col(c) <= F.lit(v),
        "eq": lambda c, v: F.col(c) == F.lit(v),
        "in": lambda c, v: F.col(c).isin(v),
        "not_null": lambda c, v: F.col(c).isNotNull()
    }

def trim_all(df: DataFrame) -> DataFrame:
    return df.select([F.trim(F.col(col_name)).alias(col_name)
                      if col_type == "string" else F.col(col_name)
                      for col_name, col_type in df.dtypes])

def trim(df: DataFrame, columns: list[str]) -> DataFrame:
    return df.select([F.trim(F.col(col_name)).alias(col_name)
                      if col_name == columns else F.col(col_name)
                      for col_name in df.columns])

def to_upper(df: DataFrame, columns: list[str]) -> DataFrame:
    return df.select([F.upper(F.col(n)).alias(n) if n in columns else F.col(n)
                      for n in df.columns])

def safe_cast(name: str, to: str) -> Column:
    return F.expr(f"try_cast(`{name}` AS {to})")

def parse_date(name: str, formats: list[str]) -> Column:
    parsed = [F.to_date(F.expr(f"try_to_timestamp(`{name}`, '{fmt}')")) for fmt in formats]
    return F.coalesce(*parsed)