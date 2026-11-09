# Data Engineering Assessment

## Overview

This repository contains a simplified version of our data pipeline architecture. We process financial instrument data using configuration-driven transformations.

Your task is to implement a working transformation engine.

## Time

~2 hours

## The Problem

Our analytics team computes derived metrics from raw instrument data. Rather than hardcoding transformation logic, we use YAML configurations that specify what operations to apply.

The configuration in `config/pipeline.yaml` defines a pipeline. Your implementation should execute it.

## Structure

```
├── config/
│   └── pipeline.yaml
├── data/
│   └── input/
├── src/
│   ├── core/
│   └── transforms/
└── tests/
```

## Your Task

Implement the transformation engine. The provided skeleton indicates roughly where code should live, but the design is yours.

## What We're Looking For

- Working code
- Production sensibilities
- Good judgment

## Prerequisites

- Python 3.10+
- Java 11, 17, or 21 (PySpark dependency)

## Running

```bash
pip install -r requirements.txt
pytest tests/
python -m src.main
```

## Questions?

Make reasonable assumptions and document them.

---

# Solution

## Design

The pipeline is driven by `config/pipeline.yaml`.

### File Responsibility

1. **`src/main.py`**: Entry point: logging, Spark session, runs the engine, prints results and rejects.
2. **`src/core/config.py`**: Loads the YAML
3. **`src/core/engine.py`**:  Orchestration: read, run steps, collect rejects, select output, validate .
4. **`src/transforms/base.py`**: `Transform` base class plus `CleanColumns`, `FilterTransform`, `ExpressionTransform`, `AggregateTransform`
5. **`src/transforms/functions.py`**:  Helpers and lookup tables (`EXPRESSION_FUNCTIONS`, `FILTER_OPERATORS`, `CHECKS`) 

### Pipeline steps

Steps run in the order they appear in the YAML. Each step has a `name` and a `type`.
1. **`clean`**: Fix values, never drops rows `trim_all`, `trim` (columns), `upper` (columns), `cast` (column, target type, date formats).
2. **`filter`**  Keep rows that pass a condition; the rest go to the rejected rows  `condition` (column, operator, value), `reject_reason`.
3. **`expression`**  Add a derived column  `multiply`, `divide`, `current_timestamp`.
4. **`aggregate`**  Group and aggregate  `group_by`, `aggregations` (sum, avg, count, min, max).
5. Supported filter operators: `gt`, `ge`, `lt`, `le`, `eq`, `in`, `not_null`.
6. Unknown step types, operators or function names raise a `ValueError` with the list of supported value.

### Data quality handling

The input data contains several anomalies. Each one is handled by rule so that **no record is silently lost**.
1. Leading/trailing spaces in `instrument_id` (e.g. `" AAPL"`)  Fixed with `trim_all` so the record joins the correct group.
2. Currency case Normalised with `upper`.
3. Price is `NULL` Rejected `missing_price`.
4. Price is `0` or negative  Rejected  `invalid_price`.
5. Volume is `0` or negative  Rejected `invalid_volume`.
6. Trade date not in `yyyy-MM-dd` Parsed using the configured formats (`yyyy-MM-dd`, `MM/dd/yyyy`)
   - Anything else becomes `NULL`.
7. Non-USD currency (EUR rows) Rejected `unsupported_currency`.
8. Exact duplicate rows (two identical META trades) Kept.

### Principles

1. Fix what is deterministic, reject what is not.
    - Whitespace and casing are fixed. 
    - Missing or invalid values are never guessed or defaulted.
2. Never drop silently.
   - Every filter step returns the rows it removes, tagged with a `reject_reason`. 
   - They are available after a run in `engine.rejected_df`.
3. Safe casting
   - `try_cast` / `try_to_timestamp` turn bad text into `NULL` instead of raising an error.

### Rejected rows

- `PipelineEngine.rejected_df` holds all rejected rows plus a `reject_reason` column. 
- `python -m src.main` prints them after the result.

### Output validation

The `validation.rules` section of the YAML is applied to the final output, after the columns are selected:
- Row-level filters reject bad input rows.
- Output validation is a second layer: 
  - If a rule fails, the final result is wrong because of a problem in the pipeline or config
  - So the run fails with a `ValueError` that lists each broken rule and the number of violating rows. 
  - All rules are evaluated in one Spark job.

## Assumptions

1. Single reporting currency (USD).
   - EUR rows are not converted and not mixed in.
   - They are rejected as `unsupported_currency`. 
   - `currency` is part of the grouping key and of the output.
2. Date format. 
   - `MM/dd/yyyy` is accepted in addition to ISO dates because the day cannot be mistaken for a month in the provided record.
3. Price and volume must be strictly positive.
   - A trade with zero volume or a non-positive price is treated as a data error, not a valid trade.
4. Duplicates are kept.
   - There is no trade id, so two identical rows cannot be told apart from two genuine identical trades. 
   - Deduplicating could silently remove real volume.
5. Metric definitions.
   - `notional = price * volume`; 
   - `avg_price` is the simple mean of trade prices; 
   - `vwap = total_notional / total_volume`, and is `NULL` rather than an error when the volume is zero.
6. Filters run before derived columns.
   - Filter steps runs before the `expression` and `aggregate` steps
   - Rejected rows from different steps are combined and need the same columns.
7. Floating point.
   - Prices are handled as `double`. 

### Testing

```bash
pytest tests/
```

### Possible improvements

- Persist rejected rows and log a summary by reason.
- Fail the run when the share of rejected rows exceeds a configurable threshold.
- Record every failed rule per row instead of only the first.
- Use `decimal` types for prices and notional values.
- Add a duplicate-row warning metric.
- Validate the YAML structure up front (required keys, referenced columns).
- Support more sources than CSV (the reader already takes its format from the config).
