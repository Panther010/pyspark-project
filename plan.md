I inspected the project files and data. I made no code changes. 
 
  ## Handling the anomalies 
 
  Treat data cleanup as explicit pipeline steps, and account for each affected row. Don’t silently drop records. 
 
  • Whitespace in instrument_id: Trim it in a normalization step. Track how many values changed, and reject or flag IDs that are blank after trimming. 
 
  • Malformed trade_date: Parse against the expected format and check for parse failures. Route invalid rows to a quarantine/reject output with a reason such as invalid_trade_date; don’t let
    them quietly disappear. 
 
  • price null, zero, or negative: These aren’t valid for the current calculations. Exclude them from metric calculations, but count and record them with a reason. Preserve the raw value in the
    rejected data. 
 
  • volume null or zero: The current pipeline uses volume as the VWAP denominator. Nulls need explicit handling; zero makes VWAP undefined. Decide whether zero-volume rows are invalid for this
    metric and quarantine or exclude them with a reason. Don’t convert nulls to zero without a business rule. 
 
  • Two currencies: Keep currency in the aggregation key. Summing USD and another currency together produces a meaningless total unless you first convert both using an agreed FX source, rate
    date, and conversion rule. 
 
  For a small ass with a reject_reason (or equivalent) and logged counts by reason is a reasonable way to demonstrate traceability. In a production pipeline,
  persist those rejects and quality metrics alongside the output, ideally with a run identifier and source information. The key is to account for input rows: accepted rows plus rejected rows
  should reconcile to the input count, allowing for any explicitly documented deduplication. 
 
  ## YAML and code responsibilities 
 
  Use YAML to describe pipeline choices and parameters: source path and format, ordered transformations, grouping keys, output columns, and validation rules. Keep the execution logic in Python:
  parsing the config, mapping supported operation names to implementations, applying them, and handling errors. Avoid putting arbitrary Python expressions in YAML; a small allowlisted set of
  operations is easier to validate and safer to maintain. 
 
  The supplied YAML already has a validation.rules section, but the skeleton doesn’t specify how validation failures should behave. Define that contract in your design: for example, whether a
  failed rule rejects rows, fails the pipeline, or records a metric. Validate config structure and referenced columns early so mistakes produce clear errors. 
 
  A sensible order for this pipeline would be: 
 
  1. Read the input while retaining enough raw data to diagnose parse errors. 
  2. Normalize fields such as instrument_id and parse/cast the date and numeric columns. 
  3. Check quality rules and separate accepted rows from rejected rows, with reasons/counts. 
  4. Calculate notional and aggregate. 
  5. Compute VWAP and validate the result. 
  6. Select the configured output columns.


   For dates and numbers, make parsing behavior explicit. If Spark infers a malformed date or numeric value as null during CSV reading, you may lose the distinction between “source was null” and
  “source value could not be parsed.” A robust approach is to read relevant fields as strings first, then cast or parse while recording failures. 
 
  ## Specific YAML questions 
 
  • Should the data issues go in YAML? Put configurable thresholds, formats, valid-value rules, and actions there if the engine is designed to support them. Keep the generic cleanup/validation
    implementations in Python. For this small assignment, a few explicit transform types such as trim, parse_date, and filter are clearer than building a large generic rules language. 
 
  • Where do validation rules belong? The existing top-level validation.rules is a reasonable place. Distinguish input checks from output checks—either by separate sections or by specifying the
    pipeline stage. For example, price and volume checks apply before calculation; vwap not_null and positive total_volume apply after aggregation. 
 
  • Should currency be in group_by? Yes, unless you normalize all values into one currency before aggregation. Also include it in the output columns. The current output requests currency but
    the aggregate groups only by instrument_id and trade_date; after aggregation, currency may be unavailable or ambiguous. Grouping by it resolves that mismatch. 
 
  ## Tests worth adding 
 
  For test_transforms.py, test each supported transform with a tiny DataFrame, including normal and edge cases: 
 
  • trim: whitespace removed; null remains null. 
  • Date parsing: valid dates parse; malformed dates are detected. 
  • Filter/validation: valid rows pass; invalid rows are counted or get the expected rejection reason. 
  • Expressions: multiplication, division, and timestamp behavior; zero denominator behavior must be defined. 
  • Aggregation: group keys and sums/averages are correct, and currency keeps groups separate. 
  • Output selection: configured column order is respected; missing configured columns fail clearly. 
 
  For pipeline tests, check more than nonempty output: 
 
  • Schema and column order match the configured output. 
  • Known input examples produce expected notional, totals, and VWAP. 
  • No duplicate instrument/date/currency keys remain. 
  • Reject counts/reasons reconcile with the input rows. 
  • Malformed configuration or unsupported operations fail with useful errors. 
 
  One test detail to revisit: test_no_duplicate_instrument_date_pairs should include currency in its uniqueness key if currency is part of the aggregation grain. 
 
  ## Suggested implementation sequence 
 
  1. Write down assumptions: accepted date format, price/volume rules, currency treatment, and reject behavior. 
  2. Implement config loading plus schema/operation validation. 
  3. Implement the smallest reusable transforms needed by the YAML. 
  4. Add row-quality handling that makes exclusions observable. 
  5. Implement pipeline execution in config order and ensure aggregation preserves all grouping keys. 
  6. Add focused transform tests, then end-to-end tests with known expected values. 
  7. Document assumptions and how rejected rows/counts are surfaced.



    ### Must have 
 
   Step                              What to do                                                                     Files                                                                        
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
   1. Set the rules                  Decide and document: trim instrument_id; parse dates using yyyy-MM-dd;         Update README.md (README.md) 
                                     require positive price and positive volume; retain rows with invalid values 
                                     in rejects and attach a reject_reason; aggregate by instrument, date, and 
                                     currency. 
  ────────────────────────────────  ─────────────────────────────────────────────────────────────────────────────  ──────────────────────────────────────────────────────────────────────────────
   2. Load and check config          Implement YAML loading. Check required sections, transformation types, and     Update src/core/config.py (src/core/config.py) 
                                     required fields. Fail early with a useful error for unsupported or 
                                     incomplete config. 
  ────────────────────────────────  ─────────────────────────────────────────────────────────────────────────────  ──────────────────────────────────────────────────────────────────────────────
   3. Read input for safe parsing    Read CSV fields in a way that lets the pipeline detect malformed dates/        Update src/core/engine.py (src/core/engine.py) 
                                     numbers rather than confusing parse failures with source nulls. 
  ────────────────────────────────  ─────────────────────────────────────────────────────────────────────────────  ──────────────────────────────────────────────────────────────────────────────
   4. Implement the transforms       Add the operations needed by the YAML: trim, validation/filtering,             Update src/transforms/functions.py (src/transforms/functions.py); add 
                                     expressions (multiply/divide/timestamp), aggregation, and output selection.    focused tests in tests/test_transforms.py (tests/test_transforms.py) 
                                     Keep supported operations explicit and limited. 
  ────────────────────────────────  ─────────────────────────────────────────────────────────────────────────────  ──────────────────────────────────────────────────────────────────────────────
   5. Execute transforms in order    Implement the engine so it reads the source and applies configured             Update src/core/engine.py (src/core/engine.py); update src/main.py (src/ 
                                     operations in order. Make the rejected rows and their reasons available to     main.py) to show output and rejection counts 
                                     the caller along with the final output. 
  ────────────────────────────────  ─────────────────────────────────────────────────────────────────────────────  ──────────────────────────────────────────────────────────────────────────────
   6. Align config with the data     Add configured cleanup and quality checks before calculations. Add currency    Update config/pipeline.yaml (config/pipeline.yaml) 
                                     to group_by, since output includes it and totals must not mix currencies. 
                                     Keep output columns explicit. 
  ────────────────────────────────  ─────────────────────────────────────────────────────────────────────────────  ──────────────────────────────────────────────────────────────────────────────
   7. Test end-to-end behavior       Check expected output values, output columns/order, unique instrument/date/    Update tests/test_pipeline.py (tests/test_pipeline.py); add or adjust 
                                     currency groups, and that invalid input rows appear in rejects with the        fixtures in tests/conftest.py (tests/conftest.py) 
                                     correct reason. 
 
  For rejection reasons, use a consistent approach: add a reject_reason column, and assign a reason for each rejected row, such as invalid_trade_date, invalid_price, or invalid_volume. Decide
  whether a row with multiple problems gets the first reason or all reasons, then test that behavior. A simple initial choice is to record all applicable reasons in one field. 
 
  The pipeline should keep the original input row count reconcilable with accepted and rejected rows. If later you add deduplication, account for those rows separately rather than silently
  losing them.

    ### Good to have 
 
   What to do                                                                                                                Files                                                               
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 
   Add tests for missing config keys, unsupported operations, null inputs, and division by zero.                             tests/test_transforms.py, tests/test_pipeline.py 
  ────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────  ────────────────────────────────────────────────────────── 
   Validate that configured input and output column names exist at the stage where they are used, and raise clear errors.    src/core/config.py, src/core/engine.py 
  ────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────  ────────────────────────────────────────────────────────── 
   Keep raw values or source columns in rejected rows so failures can be investigated.                                       src/core/engine.py, possibly src/transforms/functions.py 
 
  ### Additional value 
 
   What to do                                                                                                         Files                                                                      
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 
   Add structured counts by rejection reason and a pipeline run identifier.                                           src/core/engine.py, src/main.py 
  ─────────────────────────────────────────────────────────────────────────────────────────────────────────────────  ────────────────────────────────────────────────────────────── 
   Add configuration for accepted date format and quality rules so they can change without editing transform code.    config/pipeline.yaml, src/core/config.py, src/core/engine.py 
  ─────────────────────────────────────────────────────────────────────────────────────────────────────────────────  ────────────────────────────────────────────────────────────── 
   Document how to run the pipeline and tests, and explain the reject behavior.                                       README.md