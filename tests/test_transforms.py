"""
Unit tests for individual transformations.
"""
import pytest
import datetime
from src.transforms.base import (
    CleanColumns,
    FilterTransform,
    ExpressionTransform,
    AggregateTransform)

# Add your transform unit tests here
def make_filter(operator, value=None):
    condition = {"column": "price", "operator": operator}
    if value is not None:
        condition["value"] = value
    return FilterTransform({"name": "price_check", "reject_reason": "bad_price", "condition": condition})

def test_filter_keeps_or_rejects_negative_rows(spark):
    df = spark.createDataFrame([(5.0,), (0.0,), (-1.0,), (-3.0,)], "price double")
    step = make_filter("gt", 0)
    assert step.apply(df).count() == 1
    assert step.rejected(df).count() == 3

def test_filter_keeps_or_rejects_null_rows(spark):
    df = spark.createDataFrame([(5.0,), (0.0,), (4.0,), (None,)], "price double")
    step = make_filter("not_null")
    assert step.apply(df).count() == 3
    assert step.rejected(df).count() == 1

def test_trim_all_removes_spaces(spark):
    df = spark.createDataFrame([(" AAPL ", "   2024-01-15 ", " USD  ")], "instrument_id string, trade_date string, currency string")
    step = CleanColumns({"name": "clean", "operations": [{"function": "trim_all"}]})
    row = step.apply(df).first()
    assert row.instrument_id == "AAPL"
    assert row.trade_date == "2024-01-15"
    assert row.currency == "USD"

def test_bad_number_becomes_null(spark):
    df = spark.createDataFrame([("abc",)], "price string")
    cast = {"function": "cast", "input": [{"column": "price", "to": "double"}]}
    step = CleanColumns({"name": "clean", "operations": [cast]})
    assert step.apply(df).first().price is None

def test_divide_by_zero_gives_null(spark):
    df = spark.createDataFrame([(10.0, 0)], "n double, d long")
    expression = {"function": "divide", "inputs": [{"column": "n"}, {"column": "d"}]}
    step = ExpressionTransform({"name": "div", "output_column": "out", "expression": expression})
    assert step.apply(df).first().out is None

def test_aggregate_keeps_currencies_separate(spark):
    df = spark.createDataFrame(
        [("G", "USD", 10.0), ("G", "USD", 20.0), ("G", "EUR", 5.0)], "id string, currency string, price double")
    step = AggregateTransform({
        "name": "agg",
        "group_by": ["id", "currency"],
        "aggregations": [{"column": "price", "function": "sum", "alias": "total"}],
    })
    assert {r.currency: r.total for r in step.apply(df).collect()} == {"USD": 30.0, "EUR": 5.0}

def test_supported_date_format_converted_to_date(spark):
    df = spark.createDataFrame([
        ("G", "2024-01-15"),
        ("G", "2024-01-17"),
        ("G", "2024-01-15"),
        ("G", "01/15/2024")], "id string, trade_date string")

    cast = {"function": "cast",
            "input": [{"column": "trade_date", "to": "date", "format": ["yyyy-MM-dd", "MM/dd/yyyy"]}]}
    step = CleanColumns({"name": "clean", "operations": [cast]})

    assert ([r.trade_date for r in step.apply(df).collect()] ==
            [datetime.date(2024, 1, 15), datetime.date(2024, 1, 17), datetime.date(2024, 1, 15), datetime.date(2024, 1, 15)])

def test_unsupported_date_format_converted_to_date(spark):
    df = spark.createDataFrame([
        ("G", "2024-01-15"),
        ("G", "01-15-2024"),
        ("G", "01/15/2024")], "id string, trade_date string")

    cast = {"function": "cast",
            "input": [{"column": "trade_date", "to": "date", "format": ["yyyy-MM-dd", "MM/dd/yyyy"]}]}
    step = CleanColumns({"name": "clean", "operations": [cast]})

    assert ([r.trade_date for r in step.apply(df).collect()]) == [datetime.date(2024, 1, 15), None, datetime.date(2024, 1, 15)]

def test_rejected_rows_have_reason(spark):
    df = spark.createDataFrame([(-1.0,)], "price double")
    step = make_filter("gt", 0)
    assert step.rejected(df).first().reject_reason == "bad_price"